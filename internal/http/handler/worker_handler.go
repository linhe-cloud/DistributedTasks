package handler

import (
	"net/http"

	"DistributedTasks/internal/repo"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerHandler struct {
	db *pgxpool.Pool
}

func NewWorkerHandler(db *pgxpool.Pool) *WorkerHandler {
	return &WorkerHandler{db: db}
}

type WorkerItem struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Queues      interface{} `json:"queues"`
	HeartbeatAt string      `json:"heartbeat_at,omitempty"`
	Status      string      `json:"status"`
	Capacity    int         `json:"capacity"`
}

// GET /api/v1/workers
func (h *WorkerHandler) ListWorkers(c *gin.Context) {
	rows, err := repo.ListWorkers(c.Request.Context(), h.db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list workers failed", "detail": err.Error()})
		return
	}
	out := make([]WorkerItem, 0, len(rows))
	for _, r := range rows {
		out = append(out, WorkerItem{
			ID:          r.ID.String(),
			Name:        r.Name,
			Queues:      r.Queues,
			HeartbeatAt: r.HeartbeatAt,
			Status:      r.Status,
			Capacity:    r.Capacity,
		})
	}
	c.JSON(http.StatusOK, gin.H{"workers": out, "count": len(out)})
}
