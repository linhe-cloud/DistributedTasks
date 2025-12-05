package worker

import (
	"DistributedTasks/internal/domain"
	"DistributedTasks/internal/lease"
	"DistributedTasks/internal/queue"
	"DistributedTasks/internal/repo"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func StartLeaseReaper(ctx context.Context, db *pgxpool.Pool, rdb *redis.Client, leaseTTL, interval time.Duration) {
	tkr := time.NewTicker(interval)
	defer tkr.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tkr.C:
			before := time.Now().Add(-2 * leaseTTL)
			runs, err := repo.ListStaleRunningRuns(ctx, db, before)
			if err != nil {
				log.Printf("lease reaper list stale runs failed: %v", err)
				continue
			}
			for _, run := range runs {
				// 若租约键仍存在，说明可能还在执行，跳过
				if _, err := rdb.Get(ctx, lease.LeaseKey(run.ID.String())).Result(); err == nil {
					// lease key still exists
					continue
				} else if err != redis.Nil {
					// Redis 临时错误，跳过本次
					log.Printf("lease reaper get lease key error: %v", err)
					continue
				}

				// 获取 Task，以便读队列名、优先级、最大重试、载荷
				t, err := repo.GetTaskByID(ctx, db, run.TaskID)
				if err != nil {
					log.Printf("lease reaper get task failed: %v", err)
					continue
				}

				// 标记当前 run 为 failed（原因：lease_expired），写 result 与 finished_at
				failInfo := map[string]any{
					"reason":    "lease_expired",
					"attempt":   run.Attempt,
					"timestamp": time.Now().Format(time.RFC3339),
				}
				binfo, _ := json.Marshal(failInfo)
				if err := repo.UpdateTaskRunResult(ctx, db, run.ID, binfo); err != nil {
					log.Printf("lease reaper update run result failed: %v", err)
				}
				if err := repo.UpdateTaskRunStatus(ctx, db, run.ID, "failed"); err != nil {
					log.Printf("lease reaper update run status failed: %v", err)
					continue
				}
				if err := repo.UpdateTaskRunFinished(ctx, db, run.ID, time.Now()); err != nil {
					log.Printf("lease reaper update run finished failed: %v", err)
				}

				// 接管策略：达到最大重试 → DLQ；否则立即重试（入 ready）
				if run.Attempt >= t.MaxRetries {
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
						TaskRunID:  run.ID,
						TaskID:     run.TaskID,
						Attempt:    run.Attempt,
						Payload:    t.Payload,
						Priority:   t.Priority,
						QueueName:  t.QueueName,
						MaxRetries: t.MaxRetries,
						Error:      failInfo,
					}
					dbuf, _ := json.Marshal(dm)
					if err := queue.EnqueueDLQ(ctx, rdb, t.QueueName, string(dbuf)); err != nil {
						log.Printf("lease reaper enqueue dlq failed: %v", err)
					}
					if err := repo.UpdateTaskStatus(ctx, db, t.ID, "failed"); err != nil {
						log.Printf("lease reaper update task failed: %v", err)
					}
					log.Printf("lease reaper: task %s moved to DLQ due to lease expired", t.ID.String())
					continue
				}

				// 未达上限：插入新的 run（attempt+1，status=queued），并立即入 ready 重试
				newRunID := uuid.New()
				if err := repo.InsertTaskRun(ctx, db, &domain.TaskRun{
					ID:      newRunID,
					TaskID:  run.TaskID,
					Attempt: run.Attempt + 1,
					Status:  "queued",
				}); err != nil {
					log.Printf("lease reaper insert new run failed: %v", err)
					continue
				}

				newMsg := struct {
					TaskRunID  uuid.UUID       `json:"task_run_id"`
					TaskID     uuid.UUID       `json:"task_id"`
					Attempt    int             `json:"attempt"`
					Payload    json.RawMessage `json:"payload"`
					Priority   int             `json:"priority"`
					QueueName  string          `json:"queue_name"`
					MaxRetries int             `json:"max_retries"`
				}{
					TaskRunID:  newRunID,
					TaskID:     run.TaskID,
					Attempt:    run.Attempt + 1,
					Payload:    t.Payload,
					Priority:   t.Priority,
					QueueName:  t.QueueName,
					MaxRetries: t.MaxRetries,
				}
				bmsg, _ := json.Marshal(newMsg)
				if err := queue.EnqueueReady(ctx, rdb, t.QueueName, string(bmsg)); err != nil {
					log.Printf("lease reaper enqueue ready failed: %v", err)
					continue
				}
				log.Printf("lease reaper: task %s re-enqueued (attempt=%d)", t.ID.String(), run.Attempt+1)
			}
		}
	}
}
