package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"DistributedTasks/internal/domain"
	"DistributedTasks/internal/lease"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/repo"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Runner struct {
	db       *pgxpool.Pool
	rdb      *redis.Client
	leaseMgr *lease.Manager
	workerID string
	leaseTTL time.Duration
}

func NewRunner(db *pgxpool.Pool, rdb *redis.Client, workerID string) *Runner {
	return &Runner{
		db:       db,
		rdb:      rdb,
		leaseMgr: lease.NewManager(rdb),
		workerID: workerID,
		leaseTTL: time.Second * 30,
	}
}

type Msg struct {
	TaskRunID  uuid.UUID       `json:"task_run_id"`
	TaskID     uuid.UUID       `json:"task_id"`
	Attempt    int             `json:"attempt"`
	Payload    json.RawMessage `json:"payload"`
	Priority   int             `json:"priority"`
	QueueName  string          `json:"queue_name"`
	MaxRetries int             `json:"max_retries"`
}

func (r *Runner) HandleRawMessage(ctx context.Context, raw string) {
	var msg Msg
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		log.Printf("json unmarshal failed: %v", err)
		return
	}
	r.handle(ctx, msg)
}

func (r *Runner) handle(ctx context.Context, msg Msg) {
	now := time.Now()

	// 置为 running，记录 worker_id 与 started_at
	if err := repo.UpdateTaskRunToRunning(ctx, r.db, msg.TaskRunID, r.workerID, now); err != nil {
		log.Printf("update task run to running failed: %v", err)
	}

	// 设置租约
	ok, err := r.leaseMgr.SetLease(ctx, msg.TaskRunID.String(), r.workerID, r.leaseTTL)
	if err != nil {
		log.Printf("set lease failed or occupied: err=%v ok=%v", err, ok)
		return
	}
	// 续租协程
	renewCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		tk := time.NewTicker(10 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-tk.C:
				_, _ = r.leaseMgr.RenewLease(ctx, msg.TaskRunID.String(), r.workerID, r.leaseTTL)
			}
		}
	}()

	// 业务执行（此处维持你现有示例策略：attempt==1 且 priority 为奇数 → 失败一次）
	// 失败策略需要具体实现
	shouldFail := (msg.Attempt == 1 && msg.Priority%2 == 1)

	// 结束时释放租约 + 写 finished_at
	defer func() {
		_, _ = r.leaseMgr.ReleaseLease(ctx, msg.TaskRunID.String(), r.workerID)
		_ = repo.UpdateTaskRunFinished(ctx, r.db, msg.TaskRunID, time.Now())
	}()

	if shouldFail {
		// 计算下次重试时间（指数退避）
		base := 5 * time.Second
		factor := 1 << (msg.Attempt - 1)
		next := time.Now().Add(time.Duration(factor) * base)

		// 标记当前 run 为 retrying + 设置 next_retry_at + 写失败原因
		if err := repo.UpdateTaskRunStatus(ctx, r.db, msg.TaskRunID, "retrying"); err != nil {
			log.Printf("update task_run status failed: %v", err)
		}
		if err := repo.UpdateTaskRunNextRetryAt(ctx, r.db, msg.TaskRunID, next); err != nil {
			log.Printf("update next_retry_at failed: %v", err)
		}
		failInfo := map[string]any{
			"reason":    "simulated_fail",
			"attempt":   msg.Attempt,
			"priority":  msg.Priority,
			"timestamp": time.Now().Format(time.RFC3339),
		}
		binfo, _ := json.Marshal(failInfo)
		if err := repo.UpdateTaskRunResult(ctx, r.db, msg.TaskRunID, binfo); err != nil {
			log.Printf("update task_run result failed: %v", err)
		}

		// 达到最大重试：入DLQ + 标记失败
		if msg.Attempt >= msg.MaxRetries {
			type dlqMsg struct {
				TaskRunID  uuid.UUID       `json:"task_run_id"`
				TaskID     uuid.UUID       `json:"task_id"`
				Attempt    int             `json:"attempt"`
				Payload    json.RawMessage `json:"payload"`
				Priority   int             `json:"priority"`
				QueueName  string          `json:"queue_name"`
				MaxRetries int             `json:"max_retries"`
				Error      map[string]any  `json:"error"`
			}
			dm := dlqMsg{
				TaskRunID:  msg.TaskRunID,
				TaskID:     msg.TaskID,
				Attempt:    msg.Attempt,
				Payload:    msg.Payload,
				Priority:   msg.Priority,
				QueueName:  msg.QueueName,
				MaxRetries: msg.MaxRetries,
				Error:      failInfo,
			}
			dbuf, _ := json.Marshal(dm)
			if err := queue.EnqueueDLQ(context.Background(), r.rdb, msg.QueueName, string(dbuf)); err != nil {
				log.Printf("enqueue dlq failed: %v", err)
			}
			if err := repo.UpdateTaskRunStatus(ctx, r.db, msg.TaskRunID, "failed"); err != nil {
				log.Printf("update task_run status failed: %v", err)
			}
			if err := repo.UpdateTaskStatus(ctx, r.db, msg.TaskID, "failed"); err != nil {
				log.Printf("update task status failed: %v", err)
			}
			log.Printf("task %s moved to DLQ", msg.TaskID.String())
			return
		}

		// 创建新的 TaskRun（attempt+1，status=queued），并加入 delayed
		newRunID := uuid.New()
		if err := repo.InsertTaskRun(ctx, r.db, &domain.TaskRun{
			ID:      newRunID,
			TaskID:  msg.TaskID,
			Attempt: msg.Attempt + 1,
			Status:  "queued",
		}); err != nil {
			log.Printf("insert new task run failed: %v", err)
			return
		}

		newMsg := Msg{
			TaskRunID:  newRunID,
			TaskID:     msg.TaskID,
			Attempt:    msg.Attempt + 1,
			Payload:    msg.Payload,
			Priority:   msg.Priority,
			QueueName:  msg.QueueName,
			MaxRetries: msg.MaxRetries,
		}
		b, _ := json.Marshal(newMsg)
		if err := queue.EnqueueDelayed(context.Background(), r.rdb, msg.QueueName, string(b), next); err != nil {
			log.Printf("enqueue delayed failed: %v", err)
			return
		}
	}

	// 标记当前 run 为 succeeded
	if err := repo.UpdateTaskRunStatus(ctx, r.db, msg.TaskRunID, "succeeded"); err != nil {
		log.Printf("update task_run failed: %v", err)
		return
	}
	if err := repo.UpdateTaskStatus(ctx, r.db, msg.TaskID, "completed"); err != nil {
		log.Printf("update task status failed: %v", err)
		return
	}
	log.Printf("task %s succeeded", msg.TaskID.String())
}
