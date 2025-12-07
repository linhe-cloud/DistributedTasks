package handler

import (
	"net/http"
	"time"

	"DistributedTasks/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type ScheduleHandler struct {
	svc *service.ScheduleService
}

func NewScheduleHandler(svc *service.ScheduleService) *ScheduleHandler {
	return &ScheduleHandler{svc: svc}
}

type createScheduleRequest struct {
	TaskTemplateID string `json:"task_template_id" binding:"required"`
	CronExpr       string `json:"cron_expr" binding:"required"`
	Timezone       string `json:"timezone" binding:"required"`
	Enabled        *bool  `json:"enabled"` // 可选，默认 true
}

// POST /api/v1/schedules
func (h *ScheduleHandler) CreateSchedule(ctx *gin.Context) {
	var req createScheduleRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	tid, err := uuid.Parse(req.TaskTemplateID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid task_template_id"})
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	id, err := h.svc.CreateSchedule(ctx.Request.Context(), service.CreateScheduleParams{
		TaskTemplateID: tid,
		CronExpr:       req.CronExpr,
		Timezone:       req.Timezone,
		Enabled:        enabled,
	})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusCreated, gin.H{
		"schedule_id": id.String(),
	})
}

type listSchedulesResponse struct {
	Schedules []scheduleDTO `json:"schedules"`
}

type scheduleDTO struct {
	ID              string  `json:"id"`
	TaskTemplateID  string  `json:"task_template_id"`
	CronExpr        string  `json:"cron_expr"`
	Timezone        string  `json:"timezone"`
	Enabled         bool    `json:"enabled"`
	LastTriggeredAt *string `json:"last_triggered_at,omitempty"`
}

// GET /api/v1/schedules?enabled=true/false
func (h *ScheduleHandler) ListSchedules(c *gin.Context) {
	var enabledPtr *bool
	if v := c.Query("enabled"); v != "" {
		val := v == "true"
		enabledPtr = &val
	}
	schedules, err := h.svc.ListSchedules(c.Request.Context(), enabledPtr)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var resp listSchedulesResponse
	for _, s := range schedules {
		var last *string
		if s.LastTriggeredAt != nil {
			str := s.LastTriggeredAt.Format(time.RFC3339)
			last = &str
		}
		resp.Schedules = append(resp.Schedules, scheduleDTO{
			ID:              s.ID.String(),
			TaskTemplateID:  s.TaskTemplateID.String(),
			CronExpr:        s.CronExpr,
			Timezone:        s.Timezone,
			Enabled:         s.Enabled,
			LastTriggeredAt: last,
		})
	}
	c.JSON(http.StatusOK, resp)
}

type toggleScheduleRequest struct {
	Enabled bool `json:"enabled"`
}

// POST /api/v1/schedules/:id/toggle
func (h *ScheduleHandler) ToggleSchedule(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var req toggleScheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.ToggleSchedule(c.Request.Context(), id, req.Enabled); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": idStr, "enabled": req.Enabled})
}
