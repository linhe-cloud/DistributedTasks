package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TaskRun struct {
	ID          uuid.UUID       `json:"id"`            // 唯一标识符ID
	TaskID      uuid.UUID       `json:"task_id"`       // 任务ID
	Attempt     int             `json:"attempt"`       // 尝试次数
	Status      string          `json:"status"`        // 状态
	WorkerID    string          `json:"worker_id"`     // 工作ID
	StartedAt   *time.Time      `json:"started_at"`    // 开始时间
	FinishedAt  *time.Time      `json:"finished_at"`   // 结束时间
	Result      json.RawMessage `json:"result"`        // 结果
	NextRetryAt *time.Time      `json:"next_retry_at"` // 下次重试时间
	CreatedAt   time.Time       `json:"created_at"`    // 创建时间
}
