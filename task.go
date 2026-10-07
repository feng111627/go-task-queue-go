package taskqueue

import (
	"context"
	"errors"
	"time"
)

// Status 是任务在其生命周期中的状态。
type Status string

const (
	// StatusPending 等待被消费者领取（首次入队或失败后等待重试）。
	StatusPending Status = "pending"
	// StatusRunning 已被某个消费者领取，正在处理中。
	StatusRunning Status = "running"
	// StatusSucceeded 处理成功（终态）。
	StatusSucceeded Status = "succeeded"
	// StatusDead 重试耗尽或遇到不可重试错误，进入死信（终态）。
	StatusDead Status = "dead"
)

// Task 是队列中调度与持久化的最小单元。
type Task struct {
	// ID 任务唯一标识（由队列生成）。
	ID string
	// IdemKey 业务幂等键，相同幂等键重复提交会被识别为重复投递。
	IdemKey string
	// Type 任务类型，用于区分不同业务处理逻辑。
	Type string
	// Payload 任务负载（原样透传给处理器）。
	Payload []byte

	// Status 当前状态。
	Status Status
	// Attempt 已处理次数（首次处理前为 0，每领取一次加 1）。
	Attempt int
	// MaxAttempts 最大处理次数（含首次），超过后进入死信。
	MaxAttempts int
	// AvailableAt 任务最早可被领取的时间（用于退避重试）。
	AvailableAt time.Time
	// LeaseUntil 租约到期时间：运行中任务在此时间前不会被其他消费者领取。
	LeaseUntil time.Time
	// LastError 最近一次失败原因；死信任务保留原始失败原因。
	LastError string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// 投递与调度相关的哨兵错误。
var (
	// ErrQueueFull 队列容量已满，入队被明确拒绝。
	ErrQueueFull = errors.New("taskqueue: queue is full")
	// ErrDuplicate 相同幂等键的任务已经提交过。
	ErrDuplicate = errors.New("taskqueue: duplicate task")
	// ErrUnknownTask 查询的任务不存在。
	ErrUnknownTask = errors.New("taskqueue: unknown task")
	// ErrClosed 队列已关闭，拒绝新操作。
	ErrClosed = errors.New("taskqueue: closed")
)

// Handler 是任务处理函数。返回 nil 表示成功；
// 返回错误通过 Retryable / Terminal 包装区分是否可重试，
// 默认（未包装）视为可重试错误。
type Handler func(ctx context.Context, t *Task) error

// taskError 对处理器返回的错误进行可重试性分类。
type taskError struct {
	err       error
	retryable bool
}

func (e *taskError) Error() string { return e.err.Error() }
func (e *taskError) Unwrap() error { return e.err }

// Retryable 把错误标记为可重试错误（受最大重试次数限制）。
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &taskError{err: err, retryable: true}
}

// Terminal 把错误标记为不可重试错误，任务将直接进入死信，不再重试。
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &taskError{err: err, retryable: false}
}

// isRetryable 判断错误是否允许重试；未分类错误默认可重试，
// 这样业务方忘记包装时失败任务也不会被静默吞掉（会走重试直至死信）。
func isRetryable(err error) bool {
	var te *taskError
	if errors.As(err, &te) {
		return te.retryable
	}
	return true
}
