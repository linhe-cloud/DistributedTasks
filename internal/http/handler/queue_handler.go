package handler

import (
	"net/http"
	"strconv"

	"DistributedTasks/internal/queue"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

type QueueHandler struct {
	rdb *redis.Client
}

func NewQueueHandler(rdb *redis.Client) *QueueHandler {
	return &QueueHandler{rdb: rdb}
}

// GET /api/v1/queues/:name/dlq
func (h *QueueHandler) ListDLQ(c *gin.Context) {
	name := c.Param("name")
	countStr := c.Query("count")
	count := int64(50)
	if countStr != "" {
		if v, err := strconv.Atoi(countStr); err == nil && v > 0 {
			count = int64(v)
		}
	}
	item, err := queue.ListDLQ(c.Request.Context(), h.rdb, name, 0, count-1)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list dlq failed", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"queue": name, "count": len(item), "items": item})
}

// POST /api/v1/queues/:name/dlq/replay
type ReplayDLQRequest struct {
	Count            int  `json:"count"`
	OverridePriority *int `json:"override_priority"`
}

func (h *QueueHandler) ReplayDLQ(c *gin.Context) {
	name := c.Param("name")
	var req ReplayDLQRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Count <= 0 {
		req.Count = 1
	}
	moved, err := queue.ReplayDLQWithOverride(c.Request.Context(), h.rdb, name, req.Count, req.OverridePriority)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "replay dlq failed", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"queue": name, "moved": moved})
}
