package service

import (
	"context"
	"encoding/json"

	"DistributedTasks/internal/domain"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/repo"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type TaskService struct {
	db  *pgxpool.Pool
	rdb *redis.Client
}

func NewTaskService(db *pgxpool.Pool, rdb *redis.Client) *TaskService {
	return &TaskService{db: db, rdb: rdb}
}

type CreateTaskParams struct {
	Name       string
	Type       string // immediate/scheduled
	QueueName  string
	Priority   int
	Payload    map[string]interface{}
	DedupKey   string
	MaxRetries int
}

func (s *TaskService) CreateTask(ctx context.Context, p CreateTaskParams) (uuid.UUID, string, error) {
	// 幂等：如果 dedup_key 命中未完成任务，直接返回
	if p.DedupKey != "" {
		if t, err := repo.FindActiveTaskByDedup(ctx, s.db, p.DedupKey); err == nil && t != nil {
			return t.ID, t.Status, nil
		}
	}

	// 构造 Task
	payloadBytes, _ := json.Marshal(p.Payload)
	taskID := uuid.New()
	status := "queued"
	if p.Type == "scheduled" {
		status = "scheduled"
	}
	t := domain.Task{
		ID:         taskID,
		Name:       p.Name,
		Type:       p.Type,
		QueueName:  p.QueueName,
		Priority:   p.Priority,
		Payload:    payloadBytes,
		MaxRetries: ifZero(p.MaxRetries, 3),
		Status:     status,
		DedupKey:   p.DedupKey,
	}

	// 插入 Task
	if err := repo.InsertTask(ctx, s.db, &t); err != nil {
		return uuid.Nil, "", err
	}

	// 即时任务插入首个 TaskRun
	var taskRunID uuid.UUID
	var attempt int
	if p.Type == "immediate" {
		tr := domain.TaskRun{
			ID:      uuid.New(),
			TaskID:  taskID,
			Attempt: 1,
			Status:  "queued",
		}
		if err := repo.InsertTaskRun(ctx, s.db, &tr); err != nil {
			return uuid.Nil, "", err
		}
		taskRunID = tr.ID
		attempt = tr.Attempt
	}

	// 构造入队消息（最小字段）
	msg := struct {
		TaskRunID  uuid.UUID       `json:"task_run_id"`
		TaskID     uuid.UUID       `json:"task_id"`
		Attempt    int             `json:"attempt"`
		Payload    json.RawMessage `json:"payload"`
		Priority   int             `json:"priority"`
		QueueName  string          `json:"queue_name"`
		MaxRetries int             `json:"max_retries"`
	}{
		TaskRunID:  taskRunID,
		TaskID:     taskID,
		Attempt:    attempt,
		Payload:    payloadBytes,
		Priority:   p.Priority,
		QueueName:  p.QueueName,
		MaxRetries: ifZero(p.MaxRetries, 3),
	}
	b, _ := json.Marshal(msg)
	// 即时队列任务入队
	if p.Type == "immediate" {
		if err := queue.EnqueueReady(ctx, s.rdb, p.QueueName, string(b)); err != nil {
			return uuid.Nil, "", err
		}
	}

	return taskID, status, nil
}

func (s *TaskService) GetTaskWithLatestRun(ctx context.Context, id uuid.UUID) (*domain.Task, *domain.TaskRun, error) {
	t, err := repo.GetTaskByID(ctx, s.db, id)
	if err != nil {
		return nil, nil, err
	}
	tr, err := repo.GetLatestRunByTaskID(ctx, s.db, id)
	if err != nil {
		// 没有运行记录不算错误
		return t, nil, nil
	}
	return t, tr, nil
}

func ifZero(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}
