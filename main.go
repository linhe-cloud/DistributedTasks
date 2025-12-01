package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AppConfig struct {
	HTTPPort    string
	PostgresDSN string
	RedisURL    string
}

func loadConfig() AppConfig {
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

func initDB(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	//连接测试
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return pool, nil
}

func initRedis(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, err
	}
	return rdb, nil
}

type App struct {
	cfg AppConfig
	db  *pgxpool.Pool
	rdb *redis.Client
}

func (a *App) healthz(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}

func (a *App) readyz(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	//检查db和redis是否ping
	if err := a.db.Ping(ctx); err != nil {
		c.JSON(500, gin.H{"ready": false, "error": "db ping failed"})
		return
	}
	if err := a.rdb.Ping(ctx).Err(); err != nil {
		c.JSON(500, gin.H{"ready": false, "error": "redis ping failed"})
		return
	}
	c.JSON(200, gin.H{"ready": true})
}

func main() {
	cfg := loadConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := initDB(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer db.Close()

	rdb, err := initRedis(ctx, cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis init failed: %v", err)
	}
	defer rdb.Close()

	app := &App{cfg: cfg, db: db, rdb: rdb}

	r := gin.Default()
	r.GET("/healthz", app.healthz)
	r.GET("/readyz", app.readyz)

	log.Printf("启动api服务在：%s端口上", ":"+loadConfig().HTTPPort)

	if err := r.Run(":" + loadConfig().HTTPPort); err != nil {
		log.Fatal(err)
	}
}
