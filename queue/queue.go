package queue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Queue 是本地异步任务队列：容量受限、支持重试/死信与持久化恢复。
//
// 投递语义：at-least-once。任务被领取后记为 leased，Ack 成功落盘后才算完成；
// 进程重启时未完成的 leased 任务会被重置为 pending 重新投递，
// 因此 Handler 必须幂等（可用 Get 查询任务是否已 Done 来去重）。
type Queue struct {
	cfg   Config
	store Store

	mu     sync.Mutex
	tasks  map[string]*Task
	closed bool
	wake   chan struct{} // 状态变化时通知阻塞中的消费者
}

// New 创建队列并从 store 恢复状态：
// 重启时处于 leased（处理中）的任务被重置为 pending，保证不丢失。
func New(cfg Config, store Store) (*Queue, error) {
	cfg = cfg.withDefaults()
	if store == nil {
		store = NewFileStore("")
	}
	tasks, err := store.Load()
	if err != nil {
		return nil, err
	}
	q := &Queue{cfg: cfg, store: store, tasks: tasks, wake: make(chan struct{}, 1)}
	now := time.Now()
	for _, t := range q.tasks {
		if t.Status == StatusLeased {
			t.Status = StatusPending
			t.NotBefore = now
			t.UpdatedAt = now
		}
	}
	if err := q.persistLocked(); err != nil {
		return nil, err
	}
	return q, nil
}

// Enqueue 投递任务。容量满返回 ErrQueueFull；ID 重复返回 ErrDuplicateTask；
// 队列已关闭返回 ErrQueueClosed。任务不会被静默丢弃。
func (q *Queue) Enqueue(t *Task) error {
	if t == nil || t.ID == "" {
		return fmt.Errorf("queue: task must have a non-empty ID")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if _, exists := q.tasks[t.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateTask, t.ID)
	}
	if q.inflightLocked() >= q.cfg.Capacity {
		return fmt.Errorf("%w (capacity=%d)", ErrQueueFull, q.cfg.Capacity)
	}
	now := time.Now()
	cp := *t
	cp.Status = StatusPending
	cp.Attempts = 0
	cp.LastError = ""
	cp.CreatedAt = now
	cp.UpdatedAt = now
	cp.NotBefore = now
	if cp.MaxAttempts <= 0 {
		cp.MaxAttempts = q.cfg.DefaultMaxAttempts
	}
	q.tasks[cp.ID] = &cp
	if err := q.persistLocked(); err != nil {
		delete(q.tasks, cp.ID)
		return err
	}
	q.signalLocked()
	return nil
}

// Next 领取一个可执行任务；同一任务同一时刻只会被一个消费者领取。
// 无任务时阻塞，直至有任务到期、ctx 取消或队列关闭（返回 ErrQueueClosed）。
func (q *Queue) Next(ctx context.Context) (*Task, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, ErrQueueClosed
		}
		now := time.Now()
		if t := q.dueLocked(now); t != nil {
			t.Status = StatusLeased
			t.Attempts++
			t.UpdatedAt = now
			if err := q.persistLocked(); err != nil {
				q.mu.Unlock()
				return nil, err
			}
			cp := *t
			q.mu.Unlock()
			return &cp, nil
		}
		wait := q.nextDelayLocked(now)
		q.mu.Unlock()

		var timer *time.Timer
		var timeout <-chan time.Time
		if wait > 0 {
			timer = time.NewTimer(wait)
			timeout = timer.C
		}
		select {
		case <-q.wake:
		case <-timeout:
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return nil, ctx.Err()
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// Ack 确认任务处理成功，状态置为 done 并落盘。
func (q *Queue) Ack(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return fmt.Errorf("queue: ack unknown task %s", id)
	}
	if t.Status != StatusLeased {
		return fmt.Errorf("queue: ack task %s in status %s", id, t.Status)
	}
	t.Status = StatusDone
	t.LastError = ""
	t.UpdatedAt = time.Now()
	return q.persistLocked()
}

// Nack 报告任务处理失败：
//   - 永久错误（Permanent 包装）或已达 MaxAttempts：进死信，LastError 保留原始失败原因；
//   - 否则置回 pending，按指数退避（封顶 MaxBackoff）安排下次可领取时间。
func (q *Queue) Nack(id string, cause error) error {
	if cause == nil {
		cause = errors.New("unknown failure")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return fmt.Errorf("queue: nack unknown task %s", id)
	}
	if t.Status != StatusLeased {
		return fmt.Errorf("queue: nack task %s in status %s", id, t.Status)
	}
	now := time.Now()
	t.LastError = cause.Error()
	t.UpdatedAt = now
	if IsPermanent(cause) || t.Attempts >= t.MaxAttempts {
		t.Status = StatusDead
	} else {
		t.Status = StatusPending
		t.NotBefore = now.Add(q.cfg.backoff(t.Attempts))
	}
	if err := q.persistLocked(); err != nil {
		return err
	}
	q.signalLocked()
	return nil
}

// Stats 返回各状态任务数量快照。
func (q *Queue) Stats() map[Status]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := map[Status]int{}
	for _, t := range q.tasks {
		stats[t.Status]++
	}
	return stats
}

// Get 返回任务快照（用于测试、观测与幂等判断）。
func (q *Queue) Get(id string) (*Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	return &cp, true
}

// Close 关闭队列并落盘；关闭后 Enqueue 返回 ErrQueueClosed，
// 阻塞中的 Next 返回 ErrQueueClosed。
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	err := q.persistLocked()
	q.signalLocked()
	if cerr := q.store.Close(); err == nil {
		err = cerr
	}
	return err
}

// inflightLocked 统计占用容量的任务数（pending + leased）。
func (q *Queue) inflightLocked() int {
	n := 0
	for _, t := range q.tasks {
		if t.Status == StatusPending || t.Status == StatusLeased {
			n++
		}
	}
	return n
}

// dueLocked 返回当前可领取的任务（按 NotBefore、CreatedAt 排序取最早者）。
func (q *Queue) dueLocked(now time.Time) *Task {
	var due []*Task
	for _, t := range q.tasks {
		if t.Status == StatusPending && !t.NotBefore.After(now) {
			due = append(due, t)
		}
	}
	if len(due) == 0 {
		return nil
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].NotBefore.Equal(due[j].NotBefore) {
			return due[i].NotBefore.Before(due[j].NotBefore)
		}
		return due[i].CreatedAt.Before(due[j].CreatedAt)
	})
	return due[0]
}

// nextDelayLocked 计算到下一个任务可领取的等待时长；无待处理任务返回 0。
func (q *Queue) nextDelayLocked(now time.Time) time.Duration {
	var earliest time.Time
	for _, t := range q.tasks {
		if t.Status != StatusPending {
			continue
		}
		if earliest.IsZero() || t.NotBefore.Before(earliest) {
			earliest = t.NotBefore
		}
	}
	if earliest.IsZero() || !earliest.After(now) {
		return 0
	}
	return earliest.Sub(now)
}

// signalLocked 非阻塞地唤醒一个等待中的消费者。
func (q *Queue) signalLocked() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *Queue) persistLocked() error {
	if err := q.store.Save(q.tasks); err != nil {
		return fmt.Errorf("queue: persist: %w", err)
	}
	return nil
}
