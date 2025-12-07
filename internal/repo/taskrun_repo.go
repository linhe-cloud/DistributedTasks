package repo

import (
	"DistributedTasks/internal/domain"
	"context"
	"time"

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
        SELECT id, task_id, attempt, status, COALESCE(worker_id, ''), started_at, finished_at, result, next_retry_at, created_at
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

// UpdateTaskRunStatus 更新任务执行记录的状态
func UpdateTaskRunStatus(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, status string) error {
	_, err := db.Exec(ctx, `
		UPDATE task_runs
		SET status=$2, updated_at=NOW()
		WHERE id=$1
	`, id, status)
	return err
}

// UpdateTaskRunNextRetryAt 更新任务执行记录的下次重试时间
func UpdateTaskRunNextRetryAt(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, next time.Time) error {
	_, err := db.Exec(ctx, `
        UPDATE task_runs
        SET next_retry_at=$2, updated_at=NOW()
        WHERE id=$1
    `, id, next)
	return err
}

// UpdateTaskRunResult 更新任务执行记录的结果
func UpdateTaskRunResult(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, result []byte) error {
	_, err := db.Exec(ctx, `
		UPDATE task_runs
		SET result=$2, updated_at=NOW()
		WHERE id=$1
	`, id, result)
	return err
}

// ListStaleRunningRuns 列出所有状态为 running 且 started_at 时间早于 before 的任务执行记录
func ListStaleRunningRuns(ctx context.Context, db *pgxpool.Pool, before time.Time) ([]domain.TaskRun, error) {
	rows, err := db.Query(ctx, `
	SELECT 
    id, task_id, attempt, status, 
    COALESCE(worker_id, ''), 
    started_at, finished_at, result, 
    next_retry_at, created_at
	FROM task_runs
	WHERE 
    status = 'running' 
    AND started_at IS NOT NULL 
    AND started_at < $1
	ORDER BY started_at ASC
	`, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.TaskRun
	for rows.Next() {
		var tr domain.TaskRun
		if err := rows.Scan(&tr.ID, &tr.TaskID, &tr.Attempt, &tr.Status, &tr.WorkerID, &tr.StartedAt, &tr.FinishedAt, &tr.Result, &tr.NextRetryAt, &tr.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, tr)
	}
	return out, nil
}

// GetMaxAttemptByTaskID 获取某个任务的最大 attempt 值
func GetMaxAttemptByTaskID(ctx context.Context, db *pgxpool.Pool, taskID uuid.UUID) (int, error) {
	var maxAttempt int
	err := db.QueryRow(ctx, `
		SELECT COALESCE(MAX(attempt), 0)
		FROM task_runs
		WHERE task_id = $1
	`, taskID).Scan(&maxAttempt)
	return maxAttempt, err
}

// ListTaskRunsByTaskID 列出某个任务的所有执行记录 (按 attempt DESC， created_at DESC)
func ListTaskRunsByTaskID(ctx context.Context, db *pgxpool.Pool, taskID uuid.UUID) ([]domain.TaskRun, error) {
	rows, err := db.Query(ctx, `
		SELECT id, task_id, attempt, status, COALESCE(worker_id, ''), started_at, finished_at, result, next_retry_at,created_at
		FROM task_runs
		WHERE task_id=$1
		ORDER BY attempt DESC, created_at DESC
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.TaskRun
	for rows.Next() {
		var tr domain.TaskRun
		if err := rows.Scan(&tr.ID, &tr.TaskID, &tr.Attempt, &tr.Status, &tr.WorkerID, &tr.StartedAt, &tr.FinishedAt, &tr.Result, &tr.NextRetryAt, &tr.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, tr)
	}
	return out, nil
}
