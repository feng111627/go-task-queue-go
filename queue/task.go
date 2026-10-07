// Package queue 提供本地异步任务队列：进程内生产者/消费者、
// 失败重试、死信、容量上限与本地持久化恢复。
package queue

import (
	"errors"
	"fmt"
	"time"
)

// Status 表示任务的生命周期状态。
type Status string

const (
	StatusPending Status = "pending" // 等待被领取
	StatusLeased  Status = "leased"  // 已被消费者领取，处理中
	StatusDone    Status = "done"    // 处理成功
	StatusDead    Status = "dead"    // 进入死信
)

// 入队相关的可区分错误原因。
var (
	// ErrQueueFull 队列容量已满，任务被明确拒绝。
	ErrQueueFull = errors.New("queue: capacity full, task rejected")
	// ErrDuplicateTask 相同 ID 的任务已存在（重复提交）。
	ErrDuplicateTask = errors.New("queue: duplicate task id")
	// ErrQueueClosed 队列已关闭。
	ErrQueueClosed = errors.New("queue: closed")
)

// PermanentError 包装不可重试的错误：任务直接进死信，不再重试。
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent 把 err 标记为不可重试错误。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent 判断错误是否不可重试。
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// Task 是队列中的任务单元。
type Task struct {
	ID         string    `json:"id"`
	Payload    string    `json:"payload"`
	Status     Status    `json:"status"`
	Attempts   int       `json:"attempts"`    // 已执行次数
	MaxAttempts int      `json:"max_attempts"` // 最大执行次数（含首次）
	LastError  string    `json:"last_error"`  // 最近一次失败原因（死信保留）
	NotBefore  time.Time `json:"not_before"`  // 重试退避：该时间前不可被领取
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Config 队列可配置项。
type Config struct {
	Capacity       int           // 队列容量上限（pending+leased）
	BaseBackoff    time.Duration // 首次重试间隔
	MaxBackoff     time.Duration // 重试间隔上限
	DefaultMaxAttempts int       // 任务默认最大执行次数
}

func (c Config) withDefaults() Config {
	if c.Capacity <= 0 {
		c.Capacity = 1024
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 50 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 5 * time.Second
	}
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = 3
	}
	return c
}

// backoff 计算第 attempt 次失败后的重试间隔（指数递增，封顶 MaxBackoff）。
func (c Config) backoff(attempt int) time.Duration {
	d := c.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= c.MaxBackoff {
			return c.MaxBackoff
		}
	}
	if d > c.MaxBackoff {
		return c.MaxBackoff
	}
	return d
}

func (c Config) String() string {
	return fmt.Sprintf("Config{Capacity:%d BaseBackoff:%s MaxBackoff:%s DefaultMaxAttempts:%d}",
		c.Capacity, c.BaseBackoff, c.MaxBackoff, c.DefaultMaxAttempts)
}
