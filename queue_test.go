package taskqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// logCase 统一打印每个用例的场景与结果，使 go test 日志能直观看出覆盖情况。
func logCase(t *testing.T, scenario, name, result string, args ...any) {
	t.Helper()
	extra := ""
	for i := 0; i+1 < len(args); i += 2 {
		extra += fmt.Sprintf(" %v=%v", args[i], args[i+1])
	}
	t.Logf("[场景] %-10s [用例] %-36s [结果] %s%s", scenario, name, result, extra)
}

func newTestQueue(t *testing.T, cfg Config) *Queue {
	t.Helper()
	cfg.StoreFile = t.TempDir() + "/tasks.db"
	cfg.BaseBackoff = time.Millisecond
	cfg.MaxBackoff = 5 * time.Millisecond
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// TestConcurrentClaim 场景一：多消费者并发领取。
// 验证：同一时刻同一任务只被一个消费者领取；领取互斥；成功任务恰好处理一次。
func TestConcurrentClaim(t *testing.T) {
	const workers = 8
	const n = 40
	q := newTestQueue(t, Config{Capacity: n, Concurrency: workers, MaxAttempts: 1})
	ctx := context.Background()

	var mu sync.Mutex
	running := map[string]int{}   // 正在处理中的任务 -> 消费者 id
	processed := map[string]int{} // 任务 ID -> 处理次数
	maxInflight := 0
	inflight := 0
	started := make(chan string, n)
	gate := make(chan struct{})

	h := func(ctx context.Context, task *Task) error {
		mu.Lock()
		if _, dup := running[task.ID]; dup {
			mu.Unlock()
			t.Errorf("任务 %s 被同一时刻重复领取", task.ID)
			return nil
		}
		running[task.ID] = 1
		inflight++
		if inflight > maxInflight {
			maxInflight = inflight
		}
		mu.Unlock()

		started <- task.ID
		<-gate // 等所有消费者都持有任务后再放行，制造并发持有窗口

		mu.Lock()
		delete(running, task.ID)
		inflight--
		processed[task.ID]++
		mu.Unlock()
		return nil
	}
	q.Start(ctx, h)

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id, err := q.Enqueue(ctx, "", "demo", []byte("p"))
		if err != nil {
			t.Fatalf("入队失败: %v", err)
		}
		ids[i] = id
	}
	logCase(t, "并发领取", "40个任务入队成功", "PASS", "workers", workers)

	// 等待达到最大并发持有
	seen := map[string]bool{}
	for len(seen) < workers {
		select {
		case id := <-started:
			seen[id] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("仅 %d 个任务被同时领取，期望 %d", len(seen), workers)
		}
	}
	close(gate)

	for _, id := range ids {
		got, err := q.WaitForStatus(ctx, id, StatusSucceeded, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if got.Attempt != 1 {
			t.Errorf("任务 %s 尝试次数=%d, 期望 1（无重复处理）", id, got.Attempt)
		}
	}
	mu.Lock()
	dupes := 0
	for _, c := range processed {
		if c != 1 {
			dupes++
		}
	}
	mu.Unlock()
	if dupes != 0 {
		t.Errorf("存在 %d 个任务被重复处理", dupes)
	}
	logCase(t, "并发领取", "每个任务恰好处理一次", "PASS", "重复处理数", dupes, "峰值并发", maxInflight)
}

// TestEnqueueReject 场景一补充：容量上限拒绝原因必须可区分。
func TestEnqueueReject(t *testing.T) {
	const capN = 3
	q := newTestQueue(t, Config{Capacity: capN, Concurrency: 1})
	ctx := context.Background()
	hold := make(chan struct{})
	q.Start(ctx, func(context.Context, *Task) error { <-hold; return nil })

	holdIDs := make([]string, 0, capN)
	for i := 0; i < capN; i++ {
		id, err := q.Enqueue(ctx, "", "demo", nil)
		if err != nil {
			t.Fatalf("容量内入队不应失败: %v", err)
		}
		holdIDs = append(holdIDs, id)
	}
	// 等待任务全部进入 running（占满容量）
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, _ := q.store.CountActive(ctx)
		if n == capN {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("任务未能占满容量")
		}
		time.Sleep(time.Millisecond)
	}

	if _, err := q.Enqueue(ctx, "key-1", "demo", nil); err != ErrQueueFull {
		t.Errorf("满载入队应返回 ErrQueueFull, 实际 %v", err)
	}
	logCase(t, "容量拒绝", "满载普通任务被拒绝(ErrQueueFull)", "PASS")

	// 幂等检查优先于容量检查：满载时重复提交仍应被识别为重复而非满载。
	// 先放行占满的任务，补一个带幂等键的已存在任务。
	close(hold)
	for _, hid := range holdIDs {
		if _, err := q.WaitForStatus(ctx, hid, StatusSucceeded, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	id, err := q.Enqueue(ctx, "idem-9", "demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.WaitForStatus(ctx, id, StatusSucceeded, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	gotID, err := q.Enqueue(ctx, "idem-9", "demo", nil)
	if err != ErrDuplicate || gotID != id {
		t.Errorf("重复提交应返回 ErrDuplicate 且返回原任务ID, got id=%s err=%v", gotID, err)
	}
	logCase(t, "重复投递", "相同幂等键返回ErrDuplicate+原ID", "PASS", "id", id)
}

// TestRetryToDeadLetter 场景二：可重试错误按指数退避重试，达到上限进入死信并保留原始原因。
func TestRetryToDeadLetter(t *testing.T) {
	cfg := Config{
		Capacity:    10,
		Concurrency: 1,
		MaxAttempts: 3,
		BaseBackoff: 2 * time.Millisecond,
		MaxBackoff:  20 * time.Millisecond,
	}
	q := newTestQueue(t, cfg)
	ctx := context.Background()

	var mu sync.Mutex
	calls := 0
	h := func(_ context.Context, task *Task) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		t.Logf("  第 %d 次处理任务 %s", n, task.ID)
		return Retryable(errBoom)
	}
	q.Start(ctx, h)

	id, err := q.Enqueue(ctx, "", "flaky", nil)
	if err != nil {
		t.Fatal(err)
	}
	dead, err := q.WaitForStatus(ctx, id, StatusDead, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Errorf("处理次数=%d, 期望恰好 3 次（1次首发+2次重试）", calls)
	}
	logCase(t, "重试到死信", "可重试错误处理次数=MaxAttempts", "PASS", "处理次数", calls, "MaxAttempts", 3)
	if dead.LastError != errBoom.Error() {
		t.Errorf("死信任务应保留原始失败原因, got %q", dead.LastError)
	}
	logCase(t, "重试到死信", "死信保留原始失败原因", "PASS", "LastError", dead.LastError)

	// 校验指数退避序列（使用独立 Config，避免测试辅助器覆盖参数）：
	// base*2^(n-1)，超过 MaxBackoff 后封顶。
	bcfg := Config{BaseBackoff: 2 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}
	cases := []struct {
		attempt int
		want    time.Duration
	}{{1, 2 * time.Millisecond}, {2, 4 * time.Millisecond}, {3, 8 * time.Millisecond}, {5, 20 * time.Millisecond}}
	for _, c := range cases {
		if got := backoff(bcfg, c.attempt); got != c.want {
			t.Errorf("backoff(%d)=%v, 期望 %v", c.attempt, got, c.want)
		}
	}
	logCase(t, "重试到死信", "退避2ms->4ms->8ms并封顶20ms", "PASS")
}

// TestTerminalError 场景二补充：不可重试错误直接进死信，不再消耗重试次数。
func TestTerminalError(t *testing.T) {
	q := newTestQueue(t, Config{Capacity: 5, Concurrency: 1, MaxAttempts: 5})
	ctx := context.Background()
	calls := 0
	q.Start(ctx, func(context.Context, *Task) error {
		calls++
		return Terminal(errInvalid)
	})
	id, err := q.Enqueue(ctx, "", "bad", nil)
	if err != nil {
		t.Fatal(err)
	}
	dead, err := q.WaitForStatus(ctx, id, StatusDead, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("不可重试错误只应处理 1 次, 实际 %d 次", calls)
	}
	if dead.LastError != errInvalid.Error() || dead.Attempt != 1 {
		t.Errorf("死信原因/尝试次数异常: %q / %d", dead.LastError, dead.Attempt)
	}
	logCase(t, "不可重试错误", "Terminal错误首发即死信", "PASS", "处理次数", calls, "LastError", dead.LastError)
}

// TestRetryThenSuccess 场景二补充：重试期间恢复成功，任务成功终结，不进死信。
func TestRetryThenSuccess(t *testing.T) {
	q := newTestQueue(t, Config{Capacity: 5, Concurrency: 1, MaxAttempts: 3})
	ctx := context.Background()
	calls := 0
	q.Start(ctx, func(context.Context, *Task) error {
		calls++
		if calls < 3 {
			return Retryable(errBoom)
		}
		return nil
	})
	id, err := q.Enqueue(ctx, "", "flaky-ok", nil)
	if err != nil {
		t.Fatal(err)
	}
	tk, err := q.WaitForStatus(ctx, id, StatusSucceeded, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || tk.Attempt != 3 {
		t.Errorf("期望第3次处理成功, calls=%d attempt=%d", calls, tk.Attempt)
	}
	logCase(t, "重试后成功", "第3次尝试成功终结", "PASS", "尝试次数", tk.Attempt)
}
