package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Handler 处理任务。返回 nil 表示成功；返回 error 表示失败（默认可重试），
// 用 Permanent 包装的错误不可重试，任务直接进死信。
type Handler func(ctx context.Context, t *Task) error

// Worker 是并发消费者池，从队列领取任务并调用 Handler。
type Worker struct {
	q       *Queue
	handler Handler
	n       int
	logger  *log.Logger

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewWorker 创建包含 n 个并发消费者的 Worker。logger 为 nil 时使用默认日志。
func NewWorker(q *Queue, n int, h Handler, logger *log.Logger) *Worker {
	if n <= 0 {
		n = 1
	}
	if logger == nil {
		logger = log.New(os.Stderr, "[worker] ", log.LstdFlags)
	}
	return &Worker{q: q, handler: h, n: n, logger: logger}
}

// Start 启动消费者协程。
func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	for i := 0; i < w.n; i++ {
		w.wg.Add(1)
		go w.loop(ctx, i)
	}
}

// Stop 停止消费者并等待在途任务处理完成。
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

func (w *Worker) loop(ctx context.Context, idx int) {
	defer w.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		t, err := w.q.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, ErrQueueClosed) {
				return
			}
			w.logger.Printf("consumer-%d next error: %v", idx, err)
			continue
		}
		w.logger.Printf("consumer-%d leased task %s (attempt %d/%d)", idx, t.ID, t.Attempts, t.MaxAttempts)
		if err := w.handler(ctx, t); err != nil {
			w.logger.Printf("consumer-%d task %s failed: %v", idx, t.ID, err)
			if nerr := w.q.Nack(t.ID, err); nerr != nil {
				w.logger.Printf("consumer-%d nack task %s error: %v", idx, t.ID, nerr)
			}
			continue
		}
		if err := w.q.Ack(t.ID); err != nil {
			w.logger.Printf("consumer-%d ack task %s error: %v", idx, t.ID, err)
			continue
		}
		w.logger.Printf("consumer-%d task %s done", idx, t.ID)
	}
}

// DedupStore 持久化「已处理任务 ID」集合，配合 Idempotent 实现消费端幂等：
// 同一任务被重复投递（重试、重启恢复、重复提交）时，副作用只执行一次。
type DedupStore struct {
	path string
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewDedupStore 创建幂等登记表，path 为空时仅内存去重。
func NewDedupStore(path string) (*DedupStore, error) {
	d := &DedupStore{path: path, seen: map[string]struct{}{}}
	if path == "" {
		return d, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return d, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read dedup store: %w", err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("queue: parse dedup store: %w", err)
	}
	for _, id := range ids {
		d.seen[id] = struct{}{}
	}
	return d, nil
}

// Seen 报告任务是否已处理过。
func (d *DedupStore) Seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[id]
	return ok
}

// Mark 记录任务已处理并落盘。
func (d *DedupStore) Mark(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[id] = struct{}{}
	if d.path == "" {
		return nil
	}
	ids := make([]string, 0, len(d.seen))
	for id := range d.seen {
		ids = append(ids, id)
	}
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("queue: marshal dedup store: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(d.path), 0o755); err != nil {
		return err
	}
	tmp := d.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("queue: write dedup store: %w", err)
	}
	if err := os.Rename(tmp, d.path); err != nil {
		return fmt.Errorf("queue: rename dedup store: %w", err)
	}
	return nil
}

// Idempotent 包装 Handler：已处理过的任务直接跳过（返回 nil），
// 保证重复投递不会产生重复副作用。处理成功后先登记再返回，
// 与 Worker 配合时登记发生在 Ack 之前。
func Idempotent(d *DedupStore, h Handler) Handler {
	return func(ctx context.Context, t *Task) error {
		if d.Seen(t.ID) {
			return nil
		}
		if err := h(ctx, t); err != nil {
			return err
		}
		return d.Mark(t.ID)
	}
}
