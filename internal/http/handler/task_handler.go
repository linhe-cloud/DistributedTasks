package handler

import (
	"log"
	"net/http"

	"DistributedTasks/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type TaskHandler struct {
	svc *service.TaskService
}

func NewTaskHandler(svc *service.TaskService) *TaskHandler {
	return &TaskHandler{svc: svc}
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
func (h *TaskHandler) CreateTask(c *gin.Context) {
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
func (h *TaskHandler) GetTaskByID(c *gin.Context) {
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

// GET /api/v1/tasks/:id/runs
func (h *TaskHandler) ListTaskRuns(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		log.Printf("failed to parse task id: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
		return
	}
	runs, err := h.svc.ListTaskRunsByTaskID(c.Request.Context(), id)
	if err != nil {
		log.Printf("failed to list task runs: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list runs failed", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"task_id": idStr, "runs": runs, "count": len(runs)})
}
