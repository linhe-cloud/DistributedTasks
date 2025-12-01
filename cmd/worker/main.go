package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"DistributedTasks/internal/config"
	"DistributedTasks/internal/db"
	"DistributedTasks/internal/domain"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/repo"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//初始化依赖
	pool, err := db.Init(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer pool.Close()

	rdb, err := queue.Connect(ctx, cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis init failed: %v", err)
	}
	defer rdb.Close()

	log.Printf("worker started, queues=%v", cfg.QueueNames)

	// 延时队列搬运器（每两秒扫描一次）
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			for _, q := range cfg.QueueNames {
				if moved, err := queue.MoveDueDelayedToReady(context.Background(), rdb, q, 100); err == nil && moved > 0 {
					log.Printf("delayed moved to ready: queue=%s count=%d", q, moved)
				}
			}
		}
	}()

	// 阻塞消费队列（BLPOP），超时为 0 表示无限阻塞
	keys := make([]string, 0, len(cfg.QueueNames))
	for _, q := range cfg.QueueNames {
		keys = append(keys, queue.ReadyKey(q))
	}

	for {
		// BLPOP 返回 [key, value]
		res, err := rdb.BLPop(context.Background(), 0, keys...).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Fatalf("BLPop error: %v", err)
			continue
		}
		if len(res) != 2 {
			continue
		}
		key, val := res[0], res[1]
		log.Printf("got message from %s: %s", key, val)

		// 解析消息
		var msg struct {
			TaskRunID  uuid.UUID       `json:"task_run_id"`
			TaskID     uuid.UUID       `json:"task_id"`
			Attempt    int             `json:"attempt"`
			Payload    json.RawMessage `json:"payload"`
			Priority   int             `json:"priority"`
			QueueName  string          `json:"queue_name"`
			MaxRetries int             `json:"max_retries"`
		}
		if err := json.Unmarshal([]byte(val), &msg); err != nil {
			log.Printf("json unmarshal failed: %v", err)
			continue
		}
		// 最小演示失败策略：attempt==1 且 priority 为奇数则失败一次
		// 写回 TaskRun 与 Task 状态
		shouldFail := (msg.Attempt == 1 && msg.Priority%2 == 1)

		if shouldFail {
			// 计算下一次重试实践：指针退避 base=5s, factor=2^(attempt-1)
			base := 5 * time.Second
			factor := 1 << (msg.Attempt - 1)
			next := time.Now().Add(time.Duration(factor) * base)

			// 标记当前 run 为 retrying + 设置 next_reetry_at
			if err := repo.UpdateTaskRunStatus(context.Background(), pool, msg.TaskRunID, "succeeded"); err != nil {
				log.Printf("update task_run failed: %v", err)
				continue
			}
			if err := repo.UpdateTaskStatus(context.Background(), pool, msg.TaskID, "completed"); err != nil {
				log.Printf("update task status failed: %v", err)
				continue
			}

			// 如果达到最大重试，则进入DLQ
			if msg.Attempt >= msg.MaxRetries {
				if err := queue.EnqueueDLQ(context.Background(), rdb, msg.QueueName, val); err != nil {
					log.Printf("enqueue dlq failed: %v", err)
				}
				if err := repo.UpdateTaskRunStatus(context.Background(), pool, msg.TaskRunID, "failed"); err != nil {
					log.Printf("update task_run failed: %v", err)
				}
				if err := repo.UpdateTaskStatus(context.Background(), pool, msg.TaskID, "failed"); err != nil {
					log.Printf("update task status failed: %v", err)
				}
				log.Printf("task %s moved to DLQ", msg.TaskID.String())
				continue
			}

			// 创建新的 TaskRun（attempt+1，status=queued）
			newRunID := uuid.New()
			if err := repo.InsertTaskRun(context.Background(), pool, &domain.TaskRun{
				ID:      newRunID,
				TaskID:  msg.TaskID,
				Attempt: msg.Attempt + 1,
				Status:  "queued",
			}); err != nil {
				log.Printf("insert task_run failed: %v", err)
				continue
			}

			// 构造新的消息并加入 delayed
			newMsg := struct {
				TaskRunID  uuid.UUID       `json:"task_run_id"`
				TaskID     uuid.UUID       `json:"task_id"`
				Attempt    int             `json:"attempt"`
				Payload    json.RawMessage `json:"payload"`
				Priority   int             `json:"priority"`
				QueueName  string          `json:"queue_name"`
				MaxRetries int             `json:"max_retries"`
			}{
				TaskRunID:  newRunID,
				TaskID:     msg.TaskID,
				Attempt:    msg.Attempt + 1,
				Payload:    msg.Payload,
				Priority:   msg.Priority,
				QueueName:  msg.QueueName,
				MaxRetries: msg.MaxRetries,
			}

			b, _ := json.Marshal(newMsg)
			if err := queue.EnqueueDelayed(context.Background(), rdb, msg.QueueName, string(b), next); err != nil {
				log.Printf("enqueue delayed failed: %v", err)
				continue
			}
			log.Printf("task %s scheduled retry at %s", msg.TaskID.String(), next.Format(time.RFC3339))
			continue
		}

		// 成功分支： 更新 TaskRun 与 Task 状态
		if err := repo.UpdateTaskRunStatus(context.Background(), pool, msg.TaskRunID, "succeeded"); err != nil {
			log.Printf("update task_run failed: %v", err)
			continue
		}
		if err := repo.UpdateTaskStatus(context.Background(), pool, msg.TaskID, "completed"); err != nil {
			log.Printf("update task status failed: %v", err)
			continue
		}
		log.Printf("task %s succeeded", msg.TaskID.String())
	}
}
