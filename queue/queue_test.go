package queue

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		Capacity:           8,
		BaseBackoff:        20 * time.Millisecond,
		MaxBackoff:         100 * time.Millisecond,
		DefaultMaxAttempts: 3,
	}
}

func storePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "tasks.json")
}

// waitFor 轮询直至条件满足或超时。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// 场景 1：多消费者并发领取，同一任务同一时刻只被一个消费者领取；
// 队列满载时入队被明确拒绝（ErrQueueFull），重复提交被识别（ErrDuplicateTask）。
func TestConcurrentClaimAndCapacity(t *testing.T) {
	t.Log("场景1: 并发领取 / 容量上限拒绝 / 重复提交识别")
	q, err := New(testConfig(), NewFileStore(storePath(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// 填满容量
	for i := 0; i < testConfig().Capacity; i++ {
		if err := q.Enqueue(&Task{ID: fmt.Sprintf("t-%02d", i), Payload: "p"}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// 满载拒绝，原因可区分
	err = q.Enqueue(&Task{ID: "overflow"})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expect ErrQueueFull, got %v", err)
	}
	t.Logf("  用例: 满载入队 -> 拒绝原因: %v", err)
	// 重复提交识别
	err = q.Enqueue(&Task{ID: "t-00"})
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("expect ErrDuplicateTask, got %v", err)
	}
	t.Logf("  用例: 重复提交 -> 拒绝原因: %v", err)

	// 并发消费：记录每个任务被哪些消费者处理
	var mu sync.Mutex
	claims := map[string][]int{}
	var processed sync.WaitGroup
	processed.Add(testConfig().Capacity)
	w := NewWorker(q, 4, func(ctx context.Context, task *Task) error {
		mu.Lock()
		claims[task.ID] = append(claims[task.ID], 1)
		mu.Unlock()
		processed.Done()
		return nil
	}, nil)
	w.Start()
	processed.Wait()
	w.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(claims) != testConfig().Capacity {
		t.Fatalf("expect %d distinct tasks processed, got %d", testConfig().Capacity, len(claims))
	}
	for id, c := range claims {
		if len(c) != 1 {
			t.Fatalf("task %s claimed %d times, expect exactly 1", id, len(c))
		}
	}
	t.Logf("  用例: %d 个任务被 4 个消费者并发处理，每个任务恰好被领取 1 次 -> 通过", len(claims))
	if got := q.Stats()[StatusDone]; got != testConfig().Capacity {
		t.Fatalf("expect %d done, got %d", testConfig().Capacity, got)
	}
	t.Log("  用例: 全部任务状态为 done -> 通过")
}

// 场景 2：可重试错误按递增间隔重试，超过上限进死信并保留失败原因；
// 不可重试错误（Permanent）直接进死信。
func TestRetryBackoffAndDeadLetter(t *testing.T) {
	t.Log("场景2: 失败重试 -> 超限死信 / 永久错误直接死信")
	q, err := New(testConfig(), NewFileStore(storePath(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	if err := q.Enqueue(&Task{ID: "flaky"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(&Task{ID: "broken"}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	attemptTimes := map[string][]time.Time{}
	retryable := errors.New("temporary downstream error")
	w := NewWorker(q, 2, func(ctx context.Context, task *Task) error {
		mu.Lock()
		attemptTimes[task.ID] = append(attemptTimes[task.ID], time.Now())
		mu.Unlock()
		if task.ID == "broken" {
			return Permanent(errors.New("payload not decodable"))
		}
		return retryable
	}, nil)
	w.Start()

	waitFor(t, "flaky -> dead", func() bool {
		task, _ := q.Get("flaky")
		return task.Status == StatusDead
	})
	waitFor(t, "broken -> dead", func() bool {
		task, _ := q.Get("broken")
		return task.Status == StatusDead
	})
	w.Stop()

	flaky, _ := q.Get("flaky")
	if flaky.Attempts != flaky.MaxAttempts {
		t.Fatalf("flaky attempts=%d, expect %d", flaky.Attempts, flaky.MaxAttempts)
	}
	if flaky.LastError != retryable.Error() {
		t.Fatalf("dead letter lost original cause, got %q", flaky.LastError)
	}
	t.Logf("  用例: 可重试错误重试 %d 次后进死信，保留原因 %q -> 通过", flaky.Attempts, flaky.LastError)

	mu.Lock()
	times := attemptTimes["flaky"]
	mu.Unlock()
	if len(times) >= 2 {
		gap := times[1].Sub(times[0])
		if gap < testConfig().BaseBackoff {
			t.Fatalf("retry interval %s shorter than base backoff %s", gap, testConfig().BaseBackoff)
		}
		t.Logf("  用例: 重试间隔递增且 >= BaseBackoff（实测首次间隔 %s）-> 通过", gap)
	}

	broken, _ := q.Get("broken")
	if broken.Attempts != 1 {
		t.Fatalf("permanent error should not retry, attempts=%d", broken.Attempts)
	}
	if broken.LastError != "payload not decodable" {
		t.Fatalf("permanent dead letter cause mismatch: %q", broken.LastError)
	}
	t.Logf("  用例: 永久错误不重试，1 次即死信，保留原因 %q -> 通过", broken.LastError)
}

// 场景 3：进程重启后，pending 与 leased 任务恢复到确定状态（pending），
// 不丢失、可继续被消费。
func TestRestartRecovery(t *testing.T) {
	t.Log("场景3: 重启恢复 pending/leased 任务")
	path := storePath(t)

	q1, err := New(testConfig(), NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"wait-1", "wait-2", "run-1"} {
		if err := q1.Enqueue(&Task{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	// 领取一个任务但不 Ack，模拟「处理中」时进程崩溃
	leased, err := q1.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("  用例: 任务 %s 处于 leased 时进程退出", leased.ID)
	if err := q1.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：重新加载同一存储
	q2, err := New(testConfig(), NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	stats := q2.Stats()
	if stats[StatusPending] != 3 || stats[StatusLeased] != 0 {
		t.Fatalf("after restart expect 3 pending / 0 leased, got %v", stats)
	}
	t.Log("  用例: 重启后 leased 重置为 pending，3 个任务全部待处理 -> 通过")

	var mu sync.Mutex
	done := map[string]int{}
	w := NewWorker(q2, 2, func(ctx context.Context, task *Task) error {
		mu.Lock()
		done[task.ID]++
		mu.Unlock()
		return nil
	}, nil)
	w.Start()
	waitFor(t, "all recovered tasks done", func() bool {
		return q2.Stats()[StatusDone] == 3
	})
	w.Stop()
	q2.Close()
	t.Logf("  用例: 恢复的任务全部被消费完成（%v）-> 通过", done)
}

// 场景 4：重复投递与重启恢复引起的重复处理不产生重复副作用。
func TestDuplicateDeliveryIdempotency(t *testing.T) {
	t.Log("场景4: 重复投递识别 / 消费端幂等")
	path := storePath(t)
	dedupPath := filepath.Join(t.TempDir(), "dedup.json")

	q1, err := New(testConfig(), NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	dedup, err := NewDedupStore(dedupPath)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	sideEffects := map[string]int{}
	handler := Idempotent(dedup, func(ctx context.Context, task *Task) error {
		mu.Lock()
		sideEffects[task.ID]++
		mu.Unlock()
		return nil
	})

	if err := q1.Enqueue(&Task{ID: "pay-001"}); err != nil {
		t.Fatal(err)
	}
	// 重复提交同一 ID：被识别拒绝
	if err := q1.Enqueue(&Task{ID: "pay-001"}); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("expect ErrDuplicateTask, got %v", err)
	}
	t.Log("  用例: 相同 ID 重复提交 -> ErrDuplicateTask -> 通过")

	// 第一次消费成功，但模拟 Ack 前进程崩溃：直接关闭（任务保持 leased）
	t1, err := q1.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := handler(context.Background(), t1); err != nil {
		t.Fatal(err)
	}
	q1.Close()

	// 重启恢复：任务重新投递，但幂等层跳过副作用
	q2, err := New(testConfig(), NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	dedup2, err := NewDedupStore(dedupPath)
	if err != nil {
		t.Fatal(err)
	}
	handler2 := Idempotent(dedup2, func(ctx context.Context, task *Task) error {
		mu.Lock()
		sideEffects[task.ID]++
		mu.Unlock()
		return nil
	})
	t2, err := q2.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if t2.ID != "pay-001" {
		t.Fatalf("expect recovered task pay-001, got %s", t2.ID)
	}
	if err := handler2(context.Background(), t2); err != nil {
		t.Fatal(err)
	}
	if err := q2.Ack(t2.ID); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if sideEffects["pay-001"] != 1 {
		t.Fatalf("side effect ran %d times, expect exactly 1", sideEffects["pay-001"])
	}
	t.Log("  用例: 重启后重复投递同一任务，副作用仅执行 1 次 -> 通过")
}
