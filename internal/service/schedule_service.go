package service

import (
	"DistributedTasks/internal/domain"
	"DistributedTasks/internal/repo"
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ScheduleService struct {
	db *pgxpool.Pool
}

func NewScheduleService(db *pgxpool.Pool) *ScheduleService {
	return &ScheduleService{db: db}
}

type CreateScheduleParams struct {
	TaskTemplateID uuid.UUID
	CronExpr       string
	Timezone       string
	Enabled        bool
}

func (s *ScheduleService) CreateSchedule(ctx context.Context, params CreateScheduleParams) (uuid.UUID, error) {
	id := uuid.New()
	sch := domain.Schedule{
		ID:             id,
		TaskTemplateID: params.TaskTemplateID,
		CronExpr:       params.CronExpr,
		Timezone:       params.Timezone,
		Enabled:        params.Enabled,
	}
	if err := repo.CreateSchedule(ctx, s.db, &sch); err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}

func (s *ScheduleService) ListSchedules(ctx context.Context, enabled *bool) ([]domain.Schedule, error) {
	return repo.ListSchedules(ctx, s.db, enabled)
}

func (s *ScheduleService) ToggleSchedule(ctx context.Context, id uuid.UUID, enabled bool) error {
	return repo.ToggleScheduleEnabled(ctx, s.db, id, enabled)
}
