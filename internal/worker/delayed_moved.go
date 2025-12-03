package worker

import (
	"context"
	"log"
	"time"

	"DistributedTasks/internal/queue"

	"github.com/redis/go-redis/v9"
)

func StartDelayedMover(ctx context.Context, rdb *redis.Client, queues []string, workerID string, interval time.Duration) {
	tkr := time.NewTicker(interval)
	defer tkr.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tkr.C:
			for _, q := range queues {
				lockKey := "lock:delayed_moved:" + q
				// 获取锁
				got, err := queue.AcquireLock(ctx, rdb, lockKey, workerID, 5*time.Second)
				if err != nil || !got {
					continue
				}
				// 移动到期的延时任务到就绪队列
				moved, err := queue.MoveDueDelayedToReadyAtomic(ctx, rdb, q, 100)
				if err != nil {
					log.Printf("move delayed failed: %v", err)
				} else if moved > 0 {
					log.Printf("delayed moved to ready: queue=%s count=%d", q, moved)
				}
				// 释放锁
				_, _ = queue.ReleaseLock(ctx, rdb, lockKey, workerID)
			}
		}
	}
}
