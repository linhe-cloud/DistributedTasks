package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type HealthHandler struct {
	db  *pgxpool.Pool
	rdb *redis.Client
}

func NewHealthHandler(db *pgxpool.Pool, rdb *redis.Client) *HealthHandler {
	return &HealthHandler{db: db, rdb: rdb}
}

// GET /healthz
func (h *HealthHandler) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// GET /readyz
func (h *HealthHandler) Readyz(c *gin.Context) {
	ctx := c.Request.Context()
	// 简单就绪检查：DB、Redis 都能 ping
	if err := h.db.Ping(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ready": false, "error": "db ping failed"})
		return
	}
	if err := h.rdb.Ping(ctx).Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ready": false, "error": "redis ping failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ready": true, "timestamp": time.Now().UTC()})
}
