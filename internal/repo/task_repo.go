package repo

import (
	"DistributedTasks/internal/domain"
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FindActiveTaskByDedup 通过去重键查找活跃任务（即还未完成或可执行的）
func FindActiveTaskByDedup(ctx context.Context, db *pgxpool.Pool, dedupKey string) (*domain.Task, error) {
	if dedupKey == "" {
		return nil, nil
	}
	row := db.QueryRow(ctx, `
		SELECT id, name, type, queue_name, priority, payload, max_retries, status, dedup_key, created_at, updated_at
        FROM tasks
        WHERE dedup_key=$1 AND status IN ('pending','scheduled','queued')
        LIMIT 1
	`, dedupKey)
	var t domain.Task
	// 验证任务是否存在
	if err := row.Scan(
		&t.ID, &t.Name, &t.Type, &t.QueueName, &t.Priority, &t.Payload, &t.MaxRetries, &t.Status, &t.DedupKey,
		&t.CreatedAt, &t.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &t, nil
}

// InsertTask 向 tasks表中插入一条新的任务记录
func InsertTask(ctx context.Context, db *pgxpool.Pool, t *domain.Task) error {
	_, err := db.Exec(ctx, `
		INSERT INTO tasks (id, name, type, queue_name, priority, payload, max_retries, status, dedup_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
	`, t.ID, t.Name, t.Type, t.QueueName, t.Priority, t.Payload, t.MaxRetries, t.Status, t.DedupKey)
	return err
}

// GetTaskByID 根据任务 ID 查询完整的任务信息
func GetTaskByID(ctx context.Context, db *pgxpool.Pool, taskID uuid.UUID) (*domain.Task, error) {
	row := db.QueryRow(ctx, `
		SELECT id, name, type, queue_name, priority, payload, max_retries, status, dedup_key, created_at, updated_at
		FROM tasks
		WHERE id=$1
	`, taskID)
	var t domain.Task
	if err := row.Scan(
		&t.ID, &t.Name, &t.Type, &t.QueueName, &t.Priority, &t.Payload, &t.MaxRetries, &t.Status, &t.DedupKey,
		&t.CreatedAt, &t.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &t, nil
}

// UpdateTaskStatus 更新任务状态
func UpdateTaskStatus(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, status string) error {
	_, err := db.Exec(ctx, `
		UPDATE tasks
		SET status=$2, updated_at=NOW()
		WHERE id=$1
	`, id, status)
	return err
}

// UpdateTaskRunToRunning 将任务执行记录置为 running，写入 worker_id 与 started_at
func UpdateTaskRunToRunning(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, workerID string, startedAt time.Time) error {
	_, err := db.Exec(ctx, `
		UPDATE task_runs
		SET status=$2, worker_id=$3, started_at=$4, updated_at=NOW()
		WHERE id=$1
	`, id, "running", workerID, startedAt)
	return err
}

// UpdateTaskRunFinished 写入 finished_at（不改变 status）
func UpdateTaskRunFinished(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, finishedAt time.Time) error {
	_, err := db.Exec(ctx, `
		UPDATE task_runs
		SET status=$2, updated_at=NOW()
		WHERE id=$1
	`, id, finishedAt)
	return err
}

//
