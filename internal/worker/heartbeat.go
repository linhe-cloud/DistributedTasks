package worker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func heartbeatKey(workerID string) string {
	return "worker:" + workerID + ":heartbeat"
}

// StartHeartbeat 周期刷新 Worker 心跳键（TTL=ttl，刷新间隔=interval）
func StartHeartbeat(ctx context.Context, rdb *redis.Client, workerID string, ttl, interval time.Duration) {
	tkr := time.NewTicker(interval)
	defer tkr.Stop()
	_ = rdb.Set(ctx, heartbeatKey(workerID), "1", ttl).Err()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tkr.C:
			_ = rdb.Set(ctx, heartbeatKey(workerID), "1", ttl).Err()
		}
	}
}
