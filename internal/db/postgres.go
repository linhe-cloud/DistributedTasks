package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
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
		`ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`,
		`CREATE TABLE IF NOT EXISTS workers (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            queues JSONB NOT NULL,
            heartbeat_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'online',
            capacity INT NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );`,
		`ALTER TABLE workers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`,
		`CREATE TABLE IF NOT EXISTS schedules (
            id UUID PRIMARY KEY,
            task_template_id UUID NOT NULL REFERENCES tasks(id),
            cron_expr VARCHAR(128) NOT NULL,
            timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            last_triggered_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );`,
		`CREATE INDEX IF NOT EXISTS idx_schedules_enabled ON schedules(enabled);`,
	}
	for _, q := range ddl {
		if _, err := pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
