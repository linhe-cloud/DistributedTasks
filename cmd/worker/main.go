package main

import (
	"context"
	"log"
	"time"

	"DistributedTasks/internal/config"
	"DistributedTasks/internal/db"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/repo"
	"DistributedTasks/internal/worker"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()

	// 使用无超时根上下文
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化阶段使用单独超时
	initCtx, initCancel := context.WithTimeout(rootCtx, 10*time.Second)
	defer initCancel()

	//初始化依赖
	pool, err := db.Connect(initCtx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer pool.Close()

	// 确保 schema（含 workers） 存在
	if err := db.EnsureSchema(initCtx, pool); err != nil {
		log.Fatalf("ensure schema failed: %v", err)
	}

	rdb, err := queue.Connect(initCtx, cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis init failed: %v", err)
	}
	defer rdb.Close()

	workerID := uuid.NewString()
	log.Printf("worker started, queues=%v, workerID=%s, concurrency=%d", cfg.QueueNames, workerID, cfg.WorkerConcurrency)

	// DB 中注册 Worker
	if wid, err := uuid.Parse(workerID); err == nil {
		_ = repo.InsertWorker(rootCtx, pool, wid, "worker-"+workerID, cfg.QueueNames, cfg.WorkerConcurrency)
		// 周期性写 DB 心跳
		go func() {
			tkr := time.NewTicker(10 * time.Second)
			for {
				select {
				case <-rootCtx.Done():
					return
				case <-tkr.C:
					_ = repo.UpdateWorkerHeartbeat(rootCtx, pool, wid)
				}
			}
		}()
	}

	// 心跳存在（30秒），租约刷新间隔（10秒）
	go worker.StartHeartbeat(rootCtx, rdb, workerID, 30*time.Second, 10*time.Second)
	// 延时队列搬运器（每两秒扫描一次）
	go worker.StartDelayedMover(rootCtx, rdb, cfg.QueueNames, workerID, 2*time.Second)
	// 租约清理器（每分钟扫描一次;阈值=2*租约TTL）
	go worker.StartLeaseReaper(rootCtx, pool, rdb, 30*time.Second, 60*time.Second)

	// 并发池与 Runner
	p := worker.NewPool(cfg.WorkerConcurrency)
	p.Start()
	defer p.Stop()
	r := worker.NewRunner(pool, rdb, workerID)

	// 准备队列 keys（BLPOP：阻塞左弹），超时为 0 表示无限阻塞
	keys := make([]string, 0, len(cfg.QueueNames))
	for _, q := range cfg.QueueNames {
		keys = append(keys, queue.ReadyKey(q))
	}

	// 阻塞消费队列
	for {
		// BLPOP 返回 [key, value]
		res, err := rdb.BLPop(rootCtx, 0, keys...).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Printf("BLPop error: %v", err)
			continue
		}
		if len(res) != 2 {
			continue
		}
		key, val := res[0], res[1]
		log.Printf("got message from %s: %s", key, val)

		// 提交到并发池处理
		raw := val
		p.Submit(func(ctx context.Context) {
			r.HandleRawMessage(ctx, raw)
		})
	}
}
