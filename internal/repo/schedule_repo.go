package repo

import (
	"context"
	"time"

	"DistributedTasks/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateSchedule 创建定时计划规则
func CreateSchedule(ctx context.Context, db *pgxpool.Pool, s *domain.Schedule) error {
	_, err := db.Exec(ctx, `
		INSERT INTO schedules (id, task_template_id, cron_expr, timezone, enabled, last_triggered_at)
        VALUES ($1, $2, $3, $4, $5, $6)
	`, s.ID, s.TaskTemplateID, s.CronExpr, s.Timezone, s.Enabled, s.LastTriggeredAt)
	return err
}

// ListSchedules 简单按 enabled 过滤 （nil 表示不过滤）
func ListSchedules(ctx context.Context, db *pgxpool.Pool, enabled *bool) ([]domain.Schedule, error) {
	query := `
		SELECT id, task_template_id, cron_expr, timezone, enabled, last_triggered_at
        FROM schedules
	`
	args := []any{}
	if enabled != nil {
		query += " WHERE enabled=$1"
		args = append(args, *enabled)
	}
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []domain.Schedule
	for rows.Next() {
		var s domain.Schedule
		if err := rows.Scan(
			&s.ID, &s.TaskTemplateID, &s.CronExpr, &s.Timezone, &s.Enabled, &s.LastTriggeredAt,
		); err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, rows.Err()
}

// GetScheduleByID 根据 ID 查询schedule
func GetScheduleByID(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) (*domain.Schedule, error) {
	row := db.QueryRow(ctx, `
		SELECT id, task_template_id, cron_expr, timezone, enabled, last_triggered_at
        FROM schedules
        WHERE id = $1
	`, id)
	var s domain.Schedule
	if err := row.Scan(
		&s.ID, &s.TaskTemplateID, &s.CronExpr, &s.Timezone, &s.Enabled, &s.LastTriggeredAt,
	); err != nil {
		return nil, err
	}
	return &s, nil
}

// UpdateScheduleLastTriggeredAt 更新定时计划规则的最后触发时间
func UpdateScheduleLastTriggeredAt(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, t time.Time) error {
	_, err := db.Exec(ctx, `
		UPDATE schedules
        SET last_triggered_at = $1
        WHERE id = $2
	`, t, id)
	return err
}

// ToggleScheduleEnabled 启停一个 schedule
func ToggleScheduleEnabled(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, enabled bool) error {
	_, err := db.Exec(ctx, `
		UPDATE schedules
        SET enabled = $1
        WHERE id = $2
	`, enabled, id)
	return err
}
