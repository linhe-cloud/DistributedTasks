package handler

import (
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

type MetricsHandler struct {
	rdb *redis.Client
}

func NewMetricsHandler(rdb *redis.Client) *MetricsHandler {
	return &MetricsHandler{rdb: rdb}
}

// GET /api/v1/metrics/scheduler
func (h *MetricsHandler) GetSchedulerMetrics(c *gin.Context) {
	ctx := c.Request.Context()
	last, err := h.rdb.HGetAll(ctx, "metrics:scheduler:last").Result()
	if err != nil {
		log.Printf("failed to get scheduler metrics: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}
	ticks, err := h.rdb.Get(ctx, "metrics:scheduler:ticks").Int64()
	if err != nil {
		log.Printf("failed to get scheduler ticks: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ticks": ticks,
		"last":  last, // 包含 time, enabled_count, catchup_count, triggered_count
	})
}

// GET /api/v1/metrics/worker
func (h *MetricsHandler) GetWorkerMetrics(c *gin.Context) {
	ctx := c.Request.Context()
	keys, _, err := h.rdb.Scan(ctx, 0, "metrics:worker:*", 1000).Result()
	if err != nil {
		log.Printf("failed to scan worker metrics keys: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}
	type item struct {
		Queue  string `json:"queue"`
		Metric string `json:"metric"`
		Value  int64  `json:"value"`
	}
	var list []item
	for _, k := range keys {
		val, _ := h.rdb.Get(ctx, k).Int64()
		parts := strings.Split(k, ":")
		if len(parts) < 5 {
			continue
		}
		list = append(list, item{
			Queue:  parts[3],
			Metric: parts[4],
			Value:  val,
		})
	}
	c.JSON(http.StatusOK, gin.H{"metrics": list, "count": len(list)})
}
