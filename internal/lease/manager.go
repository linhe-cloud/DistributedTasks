package lease

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func LeaseKey(taskRunID string) string {
	return "lease:" + taskRunID
}

type Manager struct {
	rdb *redis.Client
}

func NewManager(rdb *redis.Client) *Manager {
	return &Manager{
		rdb: rdb,
	}
}

// SetLease 尝试设置租约（仅当不存在时成功），返回是否成功
func (m *Manager) SetLease(ctx context.Context, taskRunID, workerID string, ttl time.Duration) (bool, error) {
	return m.rdb.SetNX(ctx, LeaseKey(taskRunID), workerID, ttl).Result()
}

// RenewLease 仅当持有者匹配时续租，返回是否成功
func (m *Manager) RenewLease(ctx context.Context, taskRunID, workerID string, ttl time.Duration) (bool, error) {
	script := `
		if redis.call('GET', KEYS[1]) == ARGV[1] then
 			return redis.call('PEXPIRE', KEYS[1], ARGV[2])
		else
			return 0
		end`

	cmd := m.rdb.Eval(ctx, script, []string{LeaseKey(taskRunID)}, workerID, int(ttl.Milliseconds()))
	if err := cmd.Err(); err != nil {
		return false, err
	}
	n, _ := cmd.Int()
	return n == 1, nil
}

// ReleaseLease 仅当持有者匹配释放租约
func (m *Manager) ReleaseLease(ctx context.Context, taskRunID, workerID string) (bool, error) {
	script := `
		if redis.call('GET', KEYS[1]) == ARGV[1] then
			return redis.call('DEL', KEYS[1])
		else
			return 0
		end`

	cmd := m.rdb.Eval(ctx, script, []string{LeaseKey(taskRunID)}, workerID)
	if err := cmd.Err(); err != nil {
		return false, err
	}
	n, _ := cmd.Int()
	return n == 1, nil
}
