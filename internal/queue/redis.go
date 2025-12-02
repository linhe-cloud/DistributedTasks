// Package queue 提供基于 Redis 的任务队列实现
// 使用 Redis List 数据结构实现 FIFO 队列，支持任务的入队和出队操作
// 支持就绪队列(ready)、延时队列(delayed)和死信队列(dlq)三种队列类型
package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReadyKey 生成redis中队列就绪的key
func ReadyKey(queueName string) string {
	return "queue:" + queueName + ":ready"
}

// DelayedKey 延时队列（ZSET）
func DelayedKey(queueName string) string {
	return "queue:" + queueName + ":delayed"
}

// DLQKey 死信队列（List）
func DLQKey(queueName string) string {
	return "queue:" + queueName + ":dlq"
}

// EnqueueReady 将任务加入就绪队列（FIFO：RPUSH + BLPOP）
func EnqueueReady(ctx context.Context, rdb *redis.Client, queueName string, payload string) error {
	return rdb.RPush(ctx, ReadyKey(queueName), payload).Err()
}

// EnqueueDelayed 将任务加入延时队列（score 为触发时间的 Unix 秒）
func EnqueueDelayed(ctx context.Context, rdb *redis.Client, queueName string, payload string, triggerAt time.Time) error {
	return rdb.ZAdd(ctx, DelayedKey(queueName), redis.Z{
		Score:  float64(triggerAt.Unix()),
		Member: payload,
	}).Err()
}

// MoveDueDelayedToReadyAtomic 原子搬运：将到期的延时任务搬运到 ready（最多 limit 条）
func MoveDueDelayedToReadyAtomic(ctx context.Context, rdb *redis.Client, queueName string, limit int) (int, error) {
	script := `
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local moved = 0
for i=1,#items do
  redis.call('ZREM', KEYS[1], items[i])
  redis.call('RPUSH', KEYS[2], items[i])
  moved = moved + 1
end
return moved
`
	cmd := rdb.Eval(ctx, script, []string{DelayedKey(queueName), ReadyKey(queueName)}, time.Now().Unix(), limit)
	if err := cmd.Err(); err != nil {
		return 0, err
	}
	moved, _ := cmd.Int()
	return moved, nil
}

// EnqueueDLQ 将任务加入死信队列
func EnqueueDLQ(ctx context.Context, rdb *redis.Client, queueName string, payload string) error {
	return rdb.RPush(ctx, DLQKey(queueName), payload).Err()
}

// ListDLQ 查看死信队列中的任务
func ListDLQ(ctx context.Context, rdb *redis.Client, queueName string, start, stop int64) ([]string, error) {
	return rdb.LRange(ctx, DLQKey(queueName), start, stop).Result()
}

// ReplayDLQWithOverride 重放前 count 条死信到 ready（可覆盖优先级）
func ReplayDLQWithOverride(ctx context.Context, rdb *redis.Client, queueName string, count int, overridePriority *int) (int, error) {
	moved := 0
	for i := 0; i < count; i++ {
		// 从死信队列头部取出任务
		val, err := rdb.LPop(ctx, DLQKey(queueName)).Result()
		if err != nil {
			if err == redis.Nil {
				// 队列为空，结束循环
				break
			}
			return moved, err
		}
		// 尝试解析并覆盖 priority
		type msg struct {
			TaskRunID  string          `json:"task_run_id"`
			TaskID     string          `json:"task_id"`
			Attempt    int             `json:"attempt"`
			Payload    json.RawMessage `json:"payload"`
			Priority   int             `json:"priority"`
			QueueName  string          `json:"queue_name"`
			MaxRetries int             `json:"max_retries"`
			Error      map[string]any  `json:"error,omitempty"`
		}
		var m msg
		if err := json.Unmarshal([]byte(val), &m); err != nil {
			m.Priority = *overridePriority
			b, _ := json.Marshal(m)
			val = string(b)
		}
		// 无条件覆盖：只要传入了 overridePriority，就以其为准
		//可实现自动判断优先级类型并处理
		if overridePriority != nil {
			m.Priority = *overridePriority
			b, _ := json.Marshal(m)
			val = string(b)
		}
		// 将任务加入就绪队列
		if err := rdb.RPush(ctx, ReadyKey(queueName), val).Err(); err != nil {
			return moved, err
		}
		moved++
	}
	return moved, nil
}

// AcquireLock 获取分布式锁（SETNX + EX）
func AcquireLock(ctx context.Context, rdb *redis.Client, key, value string, ttl time.Duration) (bool, error) {
	return rdb.SetNX(ctx, key, value, ttl).Result()
}

// ReleaseLock 释放分布式锁（仅当 value 匹配）
func ReleaseLock(ctx context.Context, rdb *redis.Client, key, value string) (bool, error) {
	script := `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
`
	cmd := rdb.Eval(ctx, script, []string{key}, value)
	if err := cmd.Err(); err != nil {
		return false, err
	}
	n, _ := cmd.Int()
	return n == 1, nil
}

// Connect 建立 Redis 连接
// 参数:
//
//	ctx: 上下文对象，用于控制连接超时
//	url: Redis 连接 URL，格式为 "redis://[:password@]host:port[/database]"
//	     例如: "redis://localhost:6379/0" 或 "redis://:password@localhost:6379/1"
//
// 返回:
//
//	*redis.Client: 成功时返回 Redis 客户端实例
//	error: 连接失败时返回错误信息
//
// 流程:
//  1. 解析 Redis URL 获取连接配置
//  2. 创建 Redis 客户端实例
//  3. 通过 PING 命令验证连接是否正常
//  4. 连接失败时自动关闭客户端并返回错误
func Connect(ctx context.Context, url string) (*redis.Client, error) {
	// 解析 Redis 连接 URL
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	// 创建 Redis 客户端
	rdb := redis.NewClient(opt)
	// 验证连接是否可用
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, err
	}
	return rdb, nil
}
