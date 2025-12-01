package config

import "os"

type AppConfig struct {
	HTTPPort    string
	PostgresDSN string
	RedisURL    string
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
	return AppConfig{HTTPPort: prot, PostgresDSN: dsn, RedisURL: redisURL}
}
