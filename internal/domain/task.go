package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID         uuid.UUID       `json:"id"`          // 唯一标识符ID
	Name       string          `json:"name"`        // 任务名称
	Type       string          `json:"type"`        // 任务类型，immediate/scheduled
	QueueName  string          `json:"queue_name"`  // 队列名称
	Priority   int             `json:"priority"`    // 优先级
	Payload    json.RawMessage `json:"payload"`     // 任务负载
	MaxRetries int             `json:"max_retries"` // 最大重试次数
	Status     string          `json:"status"`      // 任务状态
	DedupKey   string          `json:"dedup_key"`   // 去重键
	CreatedAt  time.Time       `json:"created_at"`  // 创建时间
	UpdatedAt  time.Time       `json:"updated_at"`  // 更新时间
}
