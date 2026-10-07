package taskqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// effectLedger 模拟业务侧副作用账本：按任务 ID 幂等，
// 即使同一任务被投递多次（重启重投），外部副作用也只执行一次。
type effectLedger struct {
	mu      sync.Mutex
	effects map[string]int
}

func newLedger() *effectLedger { return &effectLedger{effects: map[string]int{}} }

func (l *effectLedger) apply(taskID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.effects[taskID] > 0 {
		return false // 副作用已存在，本次执行被幂等拦截
	}
	l.effects[taskID] = 1
	return true
}

func (l *effectLedger) count(taskID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.effects[taskID]
}

// TestDuplicateSubmitNoDoubleSideEffect 场景四：
//  1. 相同幂等键重复提交：只识别为 ErrDuplicate，不产生第二个任务；
//  2. 重启导致同一任务重复处理：处理器按 Task.ID 幂等，副作用只发生一次。
func TestDuplicateSubmitNoDoubleSideEffect(t *testing.T) {
	file := t.TempDir() + "/tasks.db"
	cfg := Config{
		Capacity: 10, Concurrency: 1, MaxAttempts: 3,
		BaseBackoff: 1e6, MaxBackoff: 5e6, // 1ms / 5ms
		StoreFile: file,
	}
	ctx := context.Background()
	ledger := newLedger()
	var realRuns int32

	// 幂等处理器：前两次处理"崩溃"（成功写入副作用前/后各一次场景），
	// 这里模拟"副作用已落账但未来得及标记任务成功"的经典至少一次重投：
	// 副作用 apply 一次后，后续重投都被幂等拦截。
	var crashBeforeAck int32 = 2 // 前 2 次投递处理后让进程崩溃/失败
	idempotentHandler := func(_ context.Context, task *Task) error {
		applied := ledger.apply(task.ID)
		if applied {
			atomicAdd(&realRuns, 1)
			t.Logf("  任务 %s 第 %d 次投递：副作用首次执行", task.ID, task.Attempt)
		} else {
			t.Logf("  任务 %s 第 %d 次投递：检测到重复，跳过副作用", task.ID, task.Attempt)
		}
		n := atomicAdd(&crashBeforeAck, -1)
		if n > 0 {
			return errBoom // 副作用已发生但本次处理失败 -> 触发重试（重复投递）
		}
		return nil
	}

	// ---- 重复提交：同幂等键 3 次提交，只有第一次真正入队 ----
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := q.Enqueue(ctx, "payment-1001", "pay", []byte(`{"amount":100}`))
	if err != nil {
		t.Fatal(err)
	}
	dupCount := 0
	for i := 0; i < 2; i++ {
		if id, derr := q.Enqueue(ctx, "payment-1001", "pay", []byte(`{"amount":100}`)); derr == ErrDuplicate && id == firstID {
			dupCount++
		} else {
			t.Fatalf("第 %d 次重复提交未被正确识别: id=%s err=%v", i+2, id, derr)
		}
	}
	activeN, _ := q.store.CountActive(ctx)
	if activeN != 1 {
		t.Fatalf("重复提交不应产生新任务, 活跃任务数=%d", activeN)
	}
	logCase(t, "重复投递", "同幂等键提交3次仅入队1个", "PASS", "重复识别次数", dupCount, "活跃任务", activeN)

	q.Start(ctx, idempotentHandler)
	task, err := q.WaitForStatus(ctx, firstID, StatusSucceeded, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Close()
	ledgerCount := ledger.count(firstID)
	if ledgerCount != 1 || realRuns != 1 {
		t.Errorf("副作用执行次数=%d 首次执行数=%d，期望都为 1（投递 %d 次）", ledgerCount, realRuns, task.Attempt)
	}
	logCase(t, "重复投递", fmt.Sprintf("失败重投%d次但副作用只执行1次", task.Attempt-1), "PASS",
		"总投递次数", task.Attempt, "副作用次数", ledgerCount)
}

// atomicAdd 是测试内的小工具，避免再开一次 import 块。
func atomicAdd(p *int32, d int32) int32 {
	return int32Add(p, d)
}
