package scheduler

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"DistributedTasks/internal/domain"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/repo"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// Scheduler 负责：周期性扫描 schedules 表，根据 cron 生成 TaskRun 并入队
type Scheduler struct {
	db       *pgxpool.Pool
	rdb      *redis.Client
	ticker   *time.Ticker
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	timezone *time.Location
}

// NewScheduler 创建一个 Scheduler
func NewScheduler(ctx context.Context, db *pgxpool.Pool, rdb *redis.Client, interval time.Duration, tz string) (*Scheduler, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithCancel(ctx)
	return &Scheduler{
		db:       db,
		rdb:      rdb,
		ticker:   time.NewTicker(interval),
		interval: interval,
		ctx:      cctx,
		cancel:   cancel,
		timezone: loc,
	}, nil
}

// Start 启动 Scheduler，每隔 interval 调用一次tickOnce
func (s *Scheduler) Start() {
	log.Printf("Scheduler started with interval=%s", s.interval)
	for {
		select {
		case <-s.ctx.Done():
			log.Println("Scheduler stopped")
			return
		case <-s.ticker.C:
			if err := s.tickOnce(s.ctx); err != nil {
				log.Printf("Error in tickOnce: %v", err)
			}
		}
	}
}

// Stop 停止 Scheduler
func (s *Scheduler) Stop() {
	s.cancel()
	s.ticker.Stop()
}

// tickOnce 扫描所有启用的 schedule, 判断是否需要触发
func (s *Scheduler) tickOnce(ctx context.Context) error {
	enabled := true
	schedules, err := repo.ListSchedules(ctx, s.db, &enabled)
	if err != nil {
		return err
	}
	now := time.Now().In(s.timezone)

	totalCatchup := 0   // 补跑的次数总和
	totalTriggered := 0 // 成功触发的新任务数总和
	for _, sch := range schedules {
		catchup, triggered, err := s.handleScheduleWithMetrics(ctx, sch, now)
		if err != nil {
			log.Printf("Error handling schedule %s: %v", sch.ID.String(), err)
			continue
		}
		totalCatchup += catchup
		totalTriggered += triggered
	}

	_ = s.rdb.Incr(ctx, "metrics:scheduler:ticks").Err()
	_ = s.rdb.HSet(ctx, "metrics:scheduler:last", map[string]any{
		"time":            now.Format(time.RFC3339),
		"enabled_count":   len(schedules),
		"catchup_count":   totalCatchup,
		"triggered_count": totalTriggered,
	}).Err()

	log.Printf("tick: enabled=%d catchup=%d triggered=%d", len(schedules), totalCatchup, totalTriggered)
	return nil
}

// handleScheduleWithMetrics 处理一个 schedule，判断是否需要触发
func (s *Scheduler) handleScheduleWithMetrics(ctx context.Context, sch domain.Schedule, now time.Time) (int, int, error) {
	// 解析 cron 表达式
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(sch.CronExpr)
	if err != nil {
		return 0, 0, err
	}

	// 补偿策略参数（可后续做成配置）
	const maxCatchupWindows = 10           // 最多补偿 10 次
	const maxCatchupDuration = time.Hour   // 只补最近 1 小时内的漏触发
	cutoff := now.Add(-maxCatchupDuration) // 补偿时间下限

	// 计算上次 trigger 时间
	var last time.Time
	if sch.LastTriggeredAt != nil {
		last = sch.LastTriggeredAt.In(s.timezone)
	} else {
		// 如果没触发过，就用当前时间往前推一个周期的“上一轮”
		last = now.Add(-s.interval)
	}

	catchupCount := 0
	triggeredCount := 0
	for {
		// 计算下次触发时间
		next := sched.Next(last)

		// 如果下次触发时间在当前时间之后，表示已经触发完毕
		if next.After(now) {
			break
		}

		// 如果下次触发时间在补偿时间下限之前，表示已经触发完毕
		if next.Before(cutoff) {
			break
		}

		// 补偿窗口次数上限，避免无限循环
		if catchupCount >= maxCatchupWindows {
			break
		}

		// 在补偿范围内：执行一次触发
		if err := s.triggerSchedule(ctx, sch, next); err != nil {
			log.Printf("trigger schedule %s at %s failed: %v", sch.ID.String(), next.Format(time.RFC3339), err)
		} else {
			triggeredCount++
		}

		// 更新 last_triggered_at
		if err := repo.UpdateScheduleLastTriggeredAt(ctx, s.db, sch.ID, next); err != nil {
			return catchupCount, triggeredCount, err
		}

		last = next
		catchupCount++
	}
	return catchupCount, triggeredCount, nil
}

// triggerSchedule 根据 schedule 生成一次 TaskRun 并入队
func (s *Scheduler) triggerSchedule(ctx context.Context, sch domain.Schedule, triggerAt time.Time) error {
	// 获取任务模板
	tplTask, err := repo.GetTaskByID(ctx, s.db, sch.TaskTemplateID)
	if err != nil {
		return err
	}
	if tplTask.Type != "scheduled" {
		log.Printf("Task template type is not scheduled:%s", tplTask.ID.String())
		return nil
	}

	// 获取该任务的最大 attempt 值，attempt = maxAttempt + 1
	maxAttempt, err := repo.GetMaxAttemptByTaskID(ctx, s.db, tplTask.ID)
	if err != nil {
		return err
	}
	nextAttempt := maxAttempt + 1

	// 创建 TaskRun 记录
	runID := uuid.New()
	tr := domain.TaskRun{
		ID:      runID,
		TaskID:  tplTask.ID,
		Attempt: nextAttempt,
		Status:  "queued",
	}
	if err := repo.InsertTaskRun(ctx, s.db, &tr); err != nil {
		return err
	}

	// 创建 TaskRun 记录
	msg := struct {
		TaskRunID  uuid.UUID       `json:"task_run_id"`
		TaskID     uuid.UUID       `json:"task_id"`
		Attempt    int             `json:"attempt"`
		Payload    json.RawMessage `json:"payload"`
		Priority   int             `json:"priority"`
		QueueName  string          `json:"queue_name"`
		MaxRetries int             `json:"max_retries"`
	}{
		TaskRunID:  runID,
		TaskID:     tplTask.ID,
		Attempt:    nextAttempt,
		Payload:    tplTask.Payload,
		Priority:   tplTask.Priority,
		QueueName:  tplTask.QueueName,
		MaxRetries: tplTask.MaxRetries,
	}
	b, _ := json.Marshal(msg)

	// 入队redis
	if err := queue.EnqueueReady(ctx, s.rdb, tplTask.QueueName, string(b)); err != nil {
		return err
	}

	log.Printf("schedule %s triggered at %s, task %s enqueued (attempt=%d)", sch.ID.String(), triggerAt.Format(time.RFC3339), tplTask.ID.String(), nextAttempt)
	return nil
}
