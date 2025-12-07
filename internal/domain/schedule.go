package domain

import (
	"time"

	"github.com/google/uuid"
)

type Schedule struct {
	ID              uuid.UUID  `json:"id"`                // 调度规则的唯一标识
	TaskTemplateID  uuid.UUID  `json:"task_template_id"`  // 关联的任务模板的唯一标识
	CronExpr        string     `json:"cron_expression"`   // cron 表达式
	Timezone        string     `json:"timezone"`          // 时区
	Enabled         bool       `json:"enabled"`           // 是否启用
	LastTriggeredAt *time.Time `json:"last_triggered_at"` // 上次触发时间（补偿）
}
