package main

import (
	"context"
	"log"
	"os"
	"time"

	"DistributedTasks/internal/config"
	"DistributedTasks/internal/db"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/scheduler"
)

func main() {
	ctx := context.Background()

	// 加载配置
	cfg := config.Load()

	// 初始化 Postgres
	pg, err := db.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect postgres failed: %v", err)
	}
	defer pg.Close()

	// 初始化 Redis
	rdb, err := queue.Connect(ctx, cfg.RedisURL)
	if err != nil {
		log.Fatalf("connect redis failed: %v", err)
	}
	defer rdb.Close()

	// 调度周期与时区
	interval := 10 * time.Second
	if v := os.Getenv("SCHEDULER_TICK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}
	tz := "Asia/Shanghai"
	sched, err := scheduler.NewScheduler(ctx, pg, rdb, interval, tz)
	if err != nil {
		log.Fatalf("new scheduler failed: %v", err)
	}

	sched.Start()
}
