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

	pool, err := db.Init(ctx, cfg.PostgresDSN)
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

	// 组装服务与路由
	taskSvc := service.NewTaskService(pool)

	engine := gin.Default()
	h := handler.New(taskSvc, pool, rdb)

	// 健康与就绪
	engine.GET("/healthz", h.Healthz)
	engine.GET("/readyz", h.Readyz)

	// 最小任务 API
	api := engine.Group("/api/v1")
	{
		api.POST("/tasks", h.CreateTask)
		api.GET("/tasks/:id", h.GetTaskByID)
	}

	log.Printf("starting api server on :%s", cfg.HTTPPort)
	if err := engine.Run(":" + cfg.HTTPPort); err != nil {
		log.Fatal(err)
	}
}
