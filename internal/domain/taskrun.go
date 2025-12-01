package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TaskRun struct {
	ID          uuid.UUID       `json:"id"`
	TaskID      uuid.UUID       `json:"task_id"`
	Attempt     int             `json:"attempt"`
	Status      string          `json:"status"`
	WorkerID    string          `json:"worker_id"`
	StartedAt   *time.Time      `json:"started_at"`
	FinishedAt  *time.Time      `json:"finished_at"`
	Result      json.RawMessage `json:"result"`
	NextRetryAt *time.Time      `json:"next_retry_at"`
	CreatedAt   time.Time       `json:"created_at"`
}
