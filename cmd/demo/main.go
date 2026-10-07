// Command demo 用进程内生产者与消费者演示本地异步任务队列：
// 并发处理、失败重试/死信、容量拒绝、幂等去重与重启恢复。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	taskqueue "github.com/feng111627/go-task-queue-go"
)

func main() {
	storeFile := "data/demo.db"
	if len(os.Args) > 1 {
		storeFile = os.Args[1]
	}

	q, err := taskqueue.New(taskqueue.Config{
		Capacity:    3,                      // 容量上限：演示满载拒绝
		MaxAttempts: 3,                      // 最多处理 3 次（首发 + 2 次重试）
		BaseBackoff: 200 * time.Millisecond, // 退避基准
		MaxBackoff:  2 * time.Second,        // 退避上限
		Concurrency: 3,                      // 3 个消费者并发
		Lease:       30 * time.Second,
		StoreFile:   storeFile,
	})
	if err != nil {
		log.Fatalf("创建队列失败: %v", err)
	}
	defer q.Close()

	// 幂等副作用账本：演示"重复投递不产生重复副作用"。
	var effects int64
	holdNormal := make(chan struct{})
	var held int64
	handler := func(_ context.Context, t *taskqueue.Task) error {
		// 前 3 个普通任务被领取后短暂阻塞，稳定占满容量以演示满载拒绝。
		if t.Type == "email" && atomic.AddInt64(&held, 1) <= 3 {
			<-holdNormal
		}
		switch t.Type {
		case "always-fail":
			// 默认（未包装）错误视为可重试，直到 MaxAttempts 后进入死信。
			return errors.New("下游服务不可用")
		case "bad-arg":
			// 不可重试错误：首发即死信。
			return taskqueue.Terminal(errors.New("参数非法"))
		default:
			atomic.AddInt64(&effects, 1)
			log.Printf("消费者处理成功: id=%s type=%s payload=%s attempt=%d",
				t.ID, t.Type, string(t.Payload), t.Attempt)
			return nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, handler)

	// ---- 生产者：正常任务 ----
	normalIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		id, err := q.Enqueue(ctx, fmt.Sprintf("idem-%d", i), "email", []byte(fmt.Sprintf("mail-%d", i)))
		if err != nil {
			log.Printf("入队被拒: %v", err)
		} else {
			normalIDs = append(normalIDs, id)
		}
	}

	// 等待 3 个 email 任务全部被领取（running），容量被占满。
	deadline := time.Now().Add(2 * time.Second)
	for n, _ := q.ActiveCount(ctx); n < 3 && time.Now().Before(deadline); n, _ = q.ActiveCount(ctx) {
		time.Sleep(time.Millisecond)
	}

	// ---- 容量已满：明确拒绝，原因可区分 ----
	if _, err := q.Enqueue(ctx, "overflow", "email", nil); err != nil {
		log.Printf("预期拒绝（满载）: %v", err)
	}

	// ---- 幂等：相同幂等键重复提交被识别（即使满载也优先判重）----
	if _, err := q.Enqueue(ctx, "idem-0", "email", nil); errors.Is(err, taskqueue.ErrDuplicate) {
		log.Printf("预期拒绝（重复提交）: %v", err)
	}
	close(holdNormal) // 放行被占住的任务，容量开始释放

	for _, id := range normalIDs {
		if _, err := q.WaitForStatus(ctx, id, taskqueue.StatusSucceeded, 3*time.Second); err != nil {
			log.Printf("等待成功任务异常: %v", err)
		}
	}
	failID, _ := q.Enqueue(ctx, "fail-1", "always-fail", nil)
	badID, _ := q.Enqueue(ctx, "bad-1", "bad-arg", nil)

	dead1, _ := q.WaitForStatus(ctx, failID, taskqueue.StatusDead, 5*time.Second)
	log.Printf("可重试任务终态: status=%s attempt=%d 原因=%q", dead1.Status, dead1.Attempt, dead1.LastError)
	dead2, _ := q.WaitForStatus(ctx, badID, taskqueue.StatusDead, 5*time.Second)
	log.Printf("不可重试任务终态: status=%s attempt=%d 原因=%q", dead2.Status, dead2.Attempt, dead2.LastError)

	log.Printf("演示结束：成功副作用次数=%d（重启后再次运行可看到 pending/running 恢复行为）", atomic.LoadInt64(&effects))
	time.Sleep(50 * time.Millisecond) // 等待最后一行日志落盘
}
