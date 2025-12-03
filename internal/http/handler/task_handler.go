package handler

import (
	"net/http"
	"strconv"
	"time"

	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Handler struct {
	svc *service.TaskService
	db  *pgxpool.Pool
	rdb *redis.Client
}

func New(svc *service.TaskService, db *pgxpool.Pool, rdb *redis.Client) *Handler {
	return &Handler{svc: svc, db: db, rdb: rdb}
}

func (h *Handler) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) Readyz(c *gin.Context) {
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

// 请求体：创建任务
type CreateTaskRequest struct {
	Name       string                 `json:"name" binding:"required"`
	Type       string                 `json:"type" binding:"required,oneof=immediate scheduled"`
	QueueName  string                 `json:"queue_name" binding:"required"`
	Priority   int                    `json:"priority"`
	Payload    map[string]interface{} `json:"payload" binding:"required"`
	DedupKey   string                 `json:"dedup_key"`
	MaxRetries int                    `json:"max_retries"`
}

// POST /api/v1/tasks
func (h *Handler) CreateTask(c *gin.Context) {
	var req CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request", "detail": err.Error()})
		return
	}

	id, status, err := h.svc.CreateTask(c.Request.Context(), service.CreateTaskParams{
		Name:       req.Name,
		Type:       req.Type,
		QueueName:  req.QueueName,
		Priority:   req.Priority,
		Payload:    req.Payload,
		DedupKey:   req.DedupKey,
		MaxRetries: req.MaxRetries,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create task failed", "detail": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"task_id": id, "status": status})
}

// GET /api/v1/tasks/:id
func (h *Handler) GetTaskByID(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
		return
	}

	t, tr, err := h.svc.GetTaskWithLatestRun(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}

	resp := gin.H{"task": t}

	displayStatus := t.Status
	if tr != nil {
		switch tr.Status {
		case "running":
			displayStatus = "running"
		case "retrying":
			displayStatus = "retrying"
		case "succeeded":
			displayStatus = "completed"
		case "failed":
			displayStatus = "failed"
		case "queued":
			displayStatus = "queued"
		case "cancelled":
			displayStatus = "cancelled"
		default:
			// 保持 task.Status
		}
	}
	resp["display_status"] = displayStatus

	if tr != nil {
		resp["latest_run"] = tr
	}
	c.JSON(http.StatusOK, resp)
}

// GET /api/v1/queues/:name/dlq
func (h *Handler) ListDLQ(c *gin.Context) {
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

func (h *Handler) ReplayDLQ(c *gin.Context) {
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
