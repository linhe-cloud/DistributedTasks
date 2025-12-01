package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Init(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	//连接测试
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return pool, nil
}

func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS tasks (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            queue_name TEXT NOT NULL,
            priority INT NOT NULL DEFAULT 0,
            payload JSONB NOT NULL,
            max_retries INT NOT NULL DEFAULT 3,
            status TEXT NOT NULL,
            dedup_key TEXT UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );`,
		`CREATE TABLE IF NOT EXISTS task_runs (
            id UUID PRIMARY KEY,
            task_id UUID NOT NULL REFERENCES tasks(id),
            attempt INT NOT NULL,
            status TEXT NOT NULL,
            worker_id TEXT,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            result JSONB,
            next_retry_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_task_attempt ON task_runs(task_id, attempt);`,
	}
	for _, q := range ddl {
		if _, err := pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
