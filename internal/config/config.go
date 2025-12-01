package config

import (
	"os"
	"strconv"
	"strings"
)

type AppConfig struct {
	HTTPPort          string
	PostgresDSN       string
	RedisURL          string
	QueueNames        []string
	WorkerConcurrency int
}

func Load() AppConfig {
	prot := os.Getenv("HTTP_PORT")
	if prot == "" {
		prot = "8080"
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "host=localhost port=5432 user=linhe dbname=task_platform sslmode=disable"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	queueEnv := os.Getenv("QUEUE_NAMES")
	var queues []string
	if queueEnv == "" {
		queues = []string{"default"}
	} else {
		// 按逗号分割队列名
		for _, q := range strings.Split(queueEnv, ",") {
			if trimmed := strings.TrimSpace(q); trimmed != "" {
				queues = append(queues, trimmed)
			}
		}
	}

	concurrency := 1
	if v := os.Getenv("WORKER_CONCURRENCY"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			concurrency = parsed
		}
	}

	return AppConfig{
		HTTPPort:          prot,
		PostgresDSN:       dsn,
		RedisURL:          redisURL,
		QueueNames:        queues,
		WorkerConcurrency: concurrency,
	}
}
