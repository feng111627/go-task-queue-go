package taskqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestRestartPendingKept 重启时 pending 的任务（退避中）恢复后仍按原计划到期处理。
func TestRestartPendingKept(t *testing.T) {
	file := t.TempDir() + "/tasks.db"
	cfg := Config{
		Capacity: 10, Concurrency: 1, MaxAttempts: 1,
		BaseBackoff: 50 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
		StoreFile: file,
	}
	ctx := context.Background()

	// 队列1只入队、不启动消费者，任务确定为 pending 落盘。
	q1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q1.Enqueue(ctx, "k-pending", "demo", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("重启前任务以 pending 状态落盘")

	// 队列2用同一文件启动并消费。
	var done int32
	var gotPayload string
	q2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	q2.Start(ctx, func(_ context.Context, task *Task) error {
		gotPayload = string(task.Payload)
		atomic.StoreInt32(&done, 1)
		return nil
	})
	task, err := q2.WaitForStatus(ctx, id, StatusSucceeded, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&done) != 1 || gotPayload != "hello" {
		t.Fatalf("pending 任务恢复后未被正确处理: done=%d payload=%q", done, gotPayload)
	}
	logCase(t, "重启恢复", "pending任务重启后恢复并处理", "PASS", "id", id, "状态", task.Status)
	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRestartRunningRedelivered 重启时 running 的任务（处理中途崩溃）
// 重置为 pending 立即重投；已消耗尝试次数保留，最终进入死信且不丢失原因。
func TestRestartRunningRedelivered(t *testing.T) {
	file := t.TempDir() + "/tasks.db"
	cfg := Config{
		Capacity: 10, Concurrency: 1, MaxAttempts: 2,
		BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
		StoreFile: file,
	}
	ctx := context.Background()

	// 队列1：任务被领取（running）、处理器第一次失败前直接"崩溃"（关闭文件存储）。
	q1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q1.Enqueue(ctx, "k-running", "flaky", nil)
	if err != nil {
		t.Fatal(err)
	}
	claimed := make(chan struct{})
	q1.Start(ctx, func(_ context.Context, task *Task) error {
		close(claimed)
		<-ctx.Done() // 模拟处理中进程被杀：一直不返回
		return ctx.Err()
	})
	select {
	case <-claimed:
	case <-time.After(2 * time.Second):
		t.Fatal("任务未被领取")
	}
	// 直接构造一个"重启现场"：任务已落盘为 running、Attempt=1。
	running, err := q1.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if running.Status != StatusRunning || running.Attempt != 1 {
		t.Fatalf("期望 running/attempt=1, got %s/%d", running.Status, running.Attempt)
	}
	// 模拟进程崩溃：不优雅退出，直接丢弃队列（底层每次领取已 fsync 落盘）。
	_ = q1.store.Close()
	t.Log("进程在任务处理中崩溃，磁盘上任务为 running，Attempt=1")

	// 队列2：同一文件重新打开，应恢复 running -> pending 并再投递一次（第2次）。
	var calls int32
	q2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := q2.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != StatusPending || rec.Attempt != 1 {
		t.Fatalf("恢复后期望 pending/attempt=1, got %s/%d", rec.Status, rec.Attempt)
	}
	logCase(t, "重启恢复", "running任务恢复为pending且保留尝试次数", "PASS", "attempt", rec.Attempt)

	q2.Start(ctx, func(context.Context, *Task) error {
		atomic.AddInt32(&calls, 1)
		return errBoom
	})
	dead, err := q2.WaitForStatus(ctx, id, StatusDead, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("恢复后只应再投递 1 次（第2次尝试）, 实际 %d", calls)
	}
	if dead.Attempt != 2 || dead.LastError != errBoom.Error() {
		t.Errorf("终态异常: attempt=%d err=%q", dead.Attempt, dead.LastError)
	}
	logCase(t, "重启恢复", "恢复任务再投一次后死信,原因不丢", "PASS", "attempt", dead.Attempt, "LastError", dead.LastError)
}
