// Package queue 提供基于 Redis 的任务队列实现
// 使用 Redis List 数据结构实现 FIFO 队列，支持任务的入队和出队操作
// 支持就绪队列(ready)、延时队列(delayed)和死信队列(dlq)三种队列类型
package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReadyKey 生成 Redis 中队列就绪状态的 key
// 参数:
//
//	queueName: 队列名称，例如 "default"、"email" 等
//
// 返回:
//
//	Redis key 格式为 "queue:{queueName}:ready"，例如 "queue:default:ready"
//
// 说明:
//
//	该 key 对应的 Redis List 存储所有待处理的任务
func ReadyKey(queueName string) string {
	return "queue:" + queueName + ":ready"
}

// DelayedKey 生成延时队列的 Redis key
// 参数:
//
//	queueName: 队列名称
//
// 返回:
//
//	Redis key 格式为 "queue:{queueName}:delayed"
//
// 说明:
//
//	延时队列使用 Redis ZSET 数据结构，Score 为任务触发时间的 Unix 时间戳
//	定时扫描器会将到期任务从延时队列移动到就绪队列
func DelayedKey(queueName string) string {
	return "queue:" + queueName + ":delayed"
}

// DLQKey 生成死信队列的 Redis key
// 参数:
//
//	queueName: 队列名称
//
// 返回:
//
//	Redis key 格式为 "queue:{queueName}:dlq" (Dead Letter Queue)
//
// 说明:
//
//	死信队列使用 Redis List 数据结构，存储执行失败且重试次数耗尽的任务
//	可用于任务审计、人工介入处理或重放到就绪队列
func DLQKey(queueName string) string {
	return "queue:" + queueName + ":dlq"
}

// EnqueueReady 将任务加入就绪队列
// 参数:
//
//	ctx: 上下文对象，用于控制超时和取消
//	rdb: Redis 客户端实例
//	queueName: 目标队列名称
//	payload: 任务负载数据，通常是 JSON 序列化后的字符串
//
// 返回:
//
//	error: 操作失败时返回错误，成功返回 nil
//
// 实现:
//
//	使用 RPUSH 命令将任务添加到队列尾部，确保 FIFO 顺序
//	Worker 通过 BLPOP 从队列头部取出任务进行处理
func EnqueueReady(ctx context.Context, rdb *redis.Client, queueName string, payload string) error {
	return rdb.RPush(ctx, ReadyKey(queueName), payload).Err()
}

// EnqueueDelayed 将任务加入延时队列
// 参数:
//
//	ctx: 上下文对象
//	rdb: Redis 客户端实例
//	queueName: 目标队列名称
//	payload: 任务负载数据
//	triggerAt: 任务触发时间点
//
// 返回:
//
//	error: 操作失败时返回错误
//
// 实现:
//
//	使用 ZADD 命令将任务添加到 ZSET，Score 为触发时间的 Unix 时间戳
//	定时扫描器会定期检查并移动到期任务到就绪队列
func EnqueueDelayed(ctx context.Context, rdb *redis.Client, queueName string, payload string, triggerAt time.Time) error {
	return rdb.ZAdd(ctx, DelayedKey(queueName), redis.Z{
		Score:  float64(triggerAt.Unix()),
		Member: payload,
	}).Err()
}

// MoveDueDelayedToReady 将到期的延时任务移动到就绪队列
// 参数:
//
//	ctx: 上下文对象
//	rdb: Redis 客户端实例
//	queueName: 目标队列名称
//	limit: 单次最多移动的任务数量，用于控制批处理大小
//
// 返回:
//
//	int: 实际移动的任务数量
//	error: 操作失败时返回错误
//
// 实现:
//  1. 使用 ZRANGEBYSCORE 查询 score <= 当前时间的所有到期任务
//  2. 使用 Pipeline 事务批量执行 ZREM 和 RPUSH 操作
//  3. 确保原子性，避免任务丢失或重复处理
//
// 使用场景:
//
//	定时扫描器(scheduler)周期性调用此函数，将到期任务激活
func MoveDueDelayedToReady(ctx context.Context, rdb *redis.Client, queueName string, limit int) (int, error) {
	now := time.Now().Unix()
	// 取到期元素 (score <= 当前时间)
	items, err := rdb.ZRangeByScore(ctx, DelayedKey(queueName), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", now),
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return 0, err
	}
	moved := 0
	// 使用 Pipeline 批量执行删除和插入操作
	pipe := rdb.TxPipeline()
	for _, m := range items {
		pipe.ZRem(ctx, DelayedKey(queueName), m) // 从延时队列移除
		pipe.RPush(ctx, ReadyKey(queueName), m)  // 加入就绪队列
		moved++
	}
	if moved == 0 {
		return 0, nil
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return moved, nil
}

// EnqueueDLQ 将任务加入死信队列
// 参数:
//
//	ctx: 上下文对象
//	rdb: Redis 客户端实例
//	queueName: 目标队列名称
//	payload: 失败任务的负载数据
//
// 返回:
//
//	error: 操作失败时返回错误
//
// 说明:
//
//	使用 RPUSH 命令将执行失败且重试次数耗尽的任务加入死信队列
//	死信队列用于保存无法正常处理的任务,便于后续人工审计和处理
func EnqueueDLQ(ctx context.Context, rdb *redis.Client, queueName string, payload string) error {
	return rdb.RPush(ctx, DLQKey(queueName), payload).Err()
}

// ListDLQ 查看死信队列中的任务
// 参数:
//
//	ctx: 上下文对象
//	rdb: Redis 客户端实例
//	queueName: 目标队列名称
//	start: 起始索引，0 表示第一个元素
//	stop: 结束索引，-1 表示最后一个元素
//
// 返回:
//
//	[]string: 任务负载数据列表
//	error: 操作失败时返回错误
//
// 说明:
//
//	使用 LRANGE 命令查询死信队列中的任务，不会移除任务
//	索引语义与 Redis LRANGE 命令相同，支持负数索引
//	例如: ListDLQ(ctx, rdb, "default", 0, 9) 返回前 10 个死信任务
func ListDLQ(ctx context.Context, rdb *redis.Client, queueName string, start, stop int64) ([]string, error) {
	return rdb.LRange(ctx, DLQKey(queueName), start, stop).Result()
}

// ReplayDLQCount 重放死信队列中的任务到就绪队列
// 参数:
//
//	ctx: 上下文对象
//	rdb: Redis 客户端实例
//	queueName: 目标队列名称
//	count: 最多重放的任务数量
//
// 返回:
//
//	int: 实际重放的任务数量
//	error: 操作失败时返回错误
//
// 实现:
//  1. 使用 LPOP 从死信队列头部取出任务
//  2. 使用 RPUSH 将任务加入就绪队列尾部
//  3. 循环处理直到达到 count 数量或死信队列为空
//
// 使用场景:
//
//	问题修复后，可以将死信任务重新加入就绪队列进行重试
//	支持批量重放，避免一次性重放过多任务导致系统压力过大
func ReplayDLQCount(ctx context.Context, rdb *redis.Client, queueName string, count int) (int, error) {
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
		// 将任务加入就绪队列
		if err := rdb.RPush(ctx, ReadyKey(queueName), val).Err(); err != nil {
			return moved, err
		}
		moved++
	}
	return moved, nil
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
