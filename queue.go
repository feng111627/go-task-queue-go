package taskqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// claimPollInterval 是消费者扫描到期任务的兜底轮询间隔。
const claimPollInterval = 10 * time.Millisecond

// Config 是队列的可配置项。
type Config struct {
	// Capacity 队列容量上限（按未终结任务 pending+running 计），必填，>0。
	Capacity int
	// MaxAttempts 每个任务的最大处理次数（含首次），默认 3。
	MaxAttempts int
	// BaseBackoff 首次重试退避基准时间，默认 1s。
	// 第 n 次失败后间隔 = min(BaseBackoff*2^(n-1), MaxBackoff)。
	BaseBackoff time.Duration
	// MaxBackoff 重试间隔上限，默认 30s。
	MaxBackoff time.Duration
	// Concurrency 消费者并发数，默认 1。
	Concurrency int
	// Lease 处理租约；仅用于崩溃恢复判定，运行中任务不会被进程内其他消费者领取，默认 30s。
	Lease time.Duration
	// StoreFile 持久化文件路径，留空使用 data/tasks.db。
	StoreFile string
}

func (c *Config) withDefaults() {
	if c.Capacity <= 0 {
		c.Capacity = 100
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.StoreFile == "" {
		c.StoreFile = "data/tasks.db"
	}
}

// Queue 是进程内异步任务队列，负责任务调度、重试与状态持久化。
type Queue struct {
	cfg   Config
	store Store

	mu     sync.Mutex
	closed bool
	stopCh chan struct{}
	ready  chan struct{} // 有新任务/状态变化时唤醒消费者（容量1）
	wg     sync.WaitGroup

	now func() time.Time // 可注入时钟，便于测试
}

// New 创建队列：打开持久化文件并恢复未完成任务到确定状态。
//   - pending 任务保持 pending（退避计划保留，到期后继续投递）；
//   - running 任务（上次进程未正常处理完）重置为 pending 并立即重新投递，
//     已消耗的尝试次数保留。
func New(cfg Config) (*Queue, error) {
	cfg.withDefaults()
	store, err := NewFileStore(cfg.StoreFile)
	if err != nil {
		return nil, err
	}
	q := &Queue{
		cfg:    cfg,
		store:  store,
		stopCh: make(chan struct{}),
		ready:  make(chan struct{}, 1),
		now:    time.Now,
	}
	if err := q.recover(context.Background()); err != nil {
		store.Close()
		return nil, err
	}
	return q, nil
}

// newWithStore 供测试注入存储与时钟。
func newWithStore(cfg Config, store Store, now func() time.Time) *Queue {
	cfg.withDefaults()
	if now == nil {
		now = time.Now
	}
	return &Queue{
		cfg:    cfg,
		store:  store,
		stopCh: make(chan struct{}),
		ready:  make(chan struct{}, 1),
		now:    now,
	}
}

func (q *Queue) recover(ctx context.Context) error {
	list, err := q.store.ListRecoverable(ctx)
	if err != nil {
		return err
	}
	for _, t := range list {
		if t.Status == StatusRunning {
			// 上次进程在处理中崩溃：回到 pending 立刻重新投递，尝试次数保留。
			t.Status = StatusPending
			t.AvailableAt = q.now()
			t.LeaseUntil = time.Time{}
			t.UpdatedAt = q.now()
			if err := q.store.UpdateTask(ctx, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Enqueue 入队一个任务，返回任务 ID。
// 拒绝原因可区分：满载返回 ErrQueueFull；相同幂等键返回 ErrDuplicate。
// 两种情况任务都不会静默进入或消失于队列。
func (q *Queue) Enqueue(ctx context.Context, idemKey, taskType string, payload []byte) (string, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return "", ErrClosed
	}
	q.mu.Unlock()

	// 1) 幂等检查优先：即使队列已满，重复提交也要被明确识别。
	if idemKey != "" {
		if existing, err := q.store.GetByIdemKey(ctx, idemKey); err == nil {
			return existing.ID, ErrDuplicate
		}
	}

	// 2) 容量检查：满载明确拒绝，由调用方决定稍后重试还是报错。
	n, err := q.store.CountActive(ctx)
	if err != nil {
		return "", err
	}
	if n >= q.cfg.Capacity {
		return "", ErrQueueFull
	}

	now := q.now()
	t := &Task{
		ID:          newID(),
		IdemKey:     idemKey,
		Type:        taskType,
		Payload:     append([]byte(nil), payload...),
		Status:      StatusPending,
		MaxAttempts: q.cfg.MaxAttempts,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := q.store.CreateTask(ctx, t); err != nil {
		return "", err // 含并发下的 ErrDuplicate
	}
	q.signal()
	return t.ID, nil
}

// signal 非阻塞唤醒消费者。
func (q *Queue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// Start 启动 cfg.Concurrency 个消费者，直到 ctx 取消或 Close 后全部退出。
func (q *Queue) Start(ctx context.Context, h Handler) {
	for i := 0; i < q.cfg.Concurrency; i++ {
		q.wg.Add(1)
		go q.worker(ctx, i, h)
	}
}

func (q *Queue) worker(ctx context.Context, id int, h Handler) {
	defer q.wg.Done()
	ticker := time.NewTicker(claimPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case <-q.ready:
		case <-ticker.C:
		}
		t, err := q.claim(ctx)
		if err != nil || t == nil {
			continue
		}
		q.execute(ctx, t, h)
	}
}

// claim 原子领取一个到期的 pending 任务：队列锁串行化领取过程，
// 保证同一任务同一时刻只会被一个消费者拿到。
func (q *Queue) claim(ctx context.Context) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	list, err := q.store.ListRecoverable(ctx)
	if err != nil {
		return nil, err
	}
	now := q.now()
	var pick *Task
	for _, t := range list {
		if t.Status != StatusPending || t.AvailableAt.After(now) {
			continue
		}
		// FIFO：取最早创建的到期任务。
		if pick == nil || t.CreatedAt.Before(pick.CreatedAt) {
			pick = t
		}
	}
	if pick == nil {
		return nil, nil
	}
	pick.Status = StatusRunning
	pick.Attempt++
	pick.LeaseUntil = now.Add(q.cfg.Lease)
	pick.UpdatedAt = now
	if err := q.store.UpdateTask(ctx, pick); err != nil {
		return nil, err
	}
	return pick, nil
}

// execute 执行业务处理并落定终态/重试态；处理器 panic 也按可重试失败处理。
func (q *Queue) execute(ctx context.Context, t *Task, h Handler) {
	err := q.runHandler(ctx, t, h)

	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	t.UpdatedAt = now
	t.LeaseUntil = time.Time{}

	switch {
	case err == nil:
		t.Status = StatusSucceeded
		t.LastError = ""
	case !isRetryable(err) || t.Attempt >= t.MaxAttempts:
		// 不可重试错误，或已达处理次数上限：进入死信，保留最后一次失败原因。
		t.Status = StatusDead
		t.LastError = err.Error()
	default:
		// 可重试且未超上限：回到 pending，按指数退避延迟重新投递。
		t.Status = StatusPending
		t.LastError = err.Error()
		t.AvailableAt = now.Add(backoff(q.cfg, t.Attempt))
	}
	_ = q.store.UpdateTask(ctx, t)
	q.signal()
}

func (q *Queue) runHandler(ctx context.Context, t *Task, h Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = Retryable(fmt.Errorf("handler panic: %v", r))
		}
	}()
	return h(ctx, t)
}

// ActiveCount 返回当前未终结（pending + running）的任务数，可用于容量观测。
func (q *Queue) ActiveCount(ctx context.Context) (int, error) {
	return q.store.CountActive(ctx)
}

// Get 按 ID 查询任务当前状态（返回副本）。
func (q *Queue) Get(ctx context.Context, id string) (*Task, error) {
	return q.store.GetTask(ctx, id)
}

// GetByIdemKey 按幂等键查询任务，便于调用方识别重复提交对应的原始任务。
func (q *Queue) GetByIdemKey(ctx context.Context, key string) (*Task, error) {
	return q.store.GetByIdemKey(ctx, key)
}

// WaitForStatus 轮询等待任务到达指定状态，超时返回错误（测试与业务同步辅助）。
func (q *Queue) WaitForStatus(ctx context.Context, id string, want Status, timeout time.Duration) (*Task, error) {
	deadline := q.now().Add(timeout)
	for {
		t, err := q.store.GetTask(ctx, id)
		if err != nil {
			return nil, err
		}
		if t.Status == want {
			return t, nil
		}
		if q.now().After(deadline) {
			return t, fmt.Errorf("taskqueue: task %s status=%s, want %s: %w", id, t.Status, want, context.DeadlineExceeded)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(claimPollInterval):
		}
	}
}

// Close 停止接收新任务并等待在途处理完成，最后关闭存储（每次写入已即时刷盘）。
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.stopCh)
	q.mu.Unlock()

	q.wg.Wait()
	return q.store.Close()
}

// backoff 计算第 attempt 次处理失败后的重试等待：base*2^(attempt-1)，封顶 max。
func backoff(cfg Config, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := cfg.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= cfg.MaxBackoff || d <= 0 { // d<=0 防御溢出
			return cfg.MaxBackoff
		}
	}
	if d > cfg.MaxBackoff {
		return cfg.MaxBackoff
	}
	return d
}
