package main

import (
	"context"
	"log"
	"time"

	"DistributedTasks/internal/config"
	"DistributedTasks/internal/db"
	"DistributedTasks/internal/http/handler"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/service"

	"github.com/gin-gonic/gin"
)

func main() {
	// 加载配置
	cfg := config.Load()

	// 初始化数据库连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := db.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer pool.Close()

	// 确保最小表结构存在
	if err := db.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("ensure schema failed: %v", err)
	}

	// 初始化 Redis
	rdb, err := queue.Connect(ctx, cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis init failed: %v", err)
	}
	defer rdb.Close()

	// 组装服务
	taskSvc := service.NewTaskService(pool, rdb)
	scheduleSvc := service.NewScheduleService(pool)

	// 组装 Handlers
	taskH := handler.NewTaskHandler(taskSvc)
	scheduleH := handler.NewScheduleHandler(scheduleSvc)
	queueH := handler.NewQueueHandler(rdb)
	workerH := handler.NewWorkerHandler(pool)
	metricsH := handler.NewMetricsHandler(rdb)
	healthH := handler.NewHealthHandler(pool, rdb)

	engine := gin.Default()

	// 健康与就绪
	engine.GET("/healthz", healthH.Healthz)
	engine.GET("/readyz", healthH.Readyz)

	// API 路由
	api := engine.Group("/api/v1")
	{
		// 任务管理
		api.POST("/tasks", taskH.CreateTask)
		api.GET("/tasks/:id", taskH.GetTaskByID)
		api.GET("/tasks/:id/runs", taskH.ListTaskRuns)

		// DLQ 管理
		api.GET("/queues/:name/dlq", queueH.ListDLQ)
		api.POST("/queues/:name/dlq/replay", queueH.ReplayDLQ)

		// Worker 管理
		api.GET("/workers", workerH.ListWorkers)

		// 监控指标
		api.GET("/metrics/scheduler", metricsH.GetSchedulerMetrics)
		api.GET("/metrics/worker", metricsH.GetWorkerMetrics)

		// 定时调度管理
		api.POST("/schedules", scheduleH.CreateSchedule)
		api.GET("/schedules", scheduleH.ListSchedules)
		api.POST("/schedules/:id/toggle", scheduleH.ToggleSchedule)
	}

	log.Printf("starting api server on :%s", cfg.HTTPPort)
	if err := engine.Run(":" + cfg.HTTPPort); err != nil {
		log.Fatal(err)
	}
}
