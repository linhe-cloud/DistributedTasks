package repo

import (
	"DistributedTasks/internal/domain"
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InsertTaskRun 插入一条新的任务执行记录（TaskRun）
func InsertTaskRun(ctx context.Context, db *pgxpool.Pool, t *domain.TaskRun) error {
	_, err := db.Exec(ctx, `
        INSERT INTO task_runs (id, task_id, attempt, status, started_at, finished_at, result, next_retry_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
    `, t.ID, t.TaskID, t.Attempt, t.Status, t.StartedAt, t.FinishedAt, t.Result, t.NextRetryAt)
	return err
}

// GetLatestRunByTaskID 获取某个任务的最新执行记录
func GetLatestRunByTaskID(ctx context.Context, db *pgxpool.Pool, taskID uuid.UUID) (*domain.TaskRun, error) {
	row := db.QueryRow(ctx, `
        SELECT id, task_id, attempt, status, worker_id, started_at, finished_at, result, next_retry_at, created_at
        FROM task_runs WHERE task_id=$1
        ORDER BY attempt DESC, created_at DESC
        LIMIT 1
    `, taskID)
	var tr domain.TaskRun
	if err := row.Scan(&tr.ID, &tr.TaskID, &tr.Attempt, &tr.Status, &tr.WorkerID, &tr.StartedAt, &tr.FinishedAt, &tr.Result, &tr.NextRetryAt, &tr.CreatedAt); err != nil {
		return nil, err
	}
	return &tr, nil
}
