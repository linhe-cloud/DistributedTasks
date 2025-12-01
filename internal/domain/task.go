package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID         uuid.UUID       `json:"id"`
	Name       string          `json:"name"`
	Type       string          `json:"type"` // immediate/scheduled
	QueueName  string          `json:"queue_name"`
	Priority   int             `json:"priority"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries int             `json:"max_retries"`
	Status     string          `json:"status"`
	DedupKey   string          `json:"dedup_key"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}
