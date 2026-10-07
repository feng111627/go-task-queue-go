// demo 用进程内生产者/消费者模拟真实投递：
// 部分任务随机失败触发重试，部分任务返回永久错误进死信，
// 最后打印各状态任务数量与死信原因。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/feng111627/go-task-queue-go/queue"
)

func main() {
	dir, err := os.MkdirTemp("", "taskq-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	q, err := queue.New(queue.Config{
		Capacity:           16,
		BaseBackoff:        100 * time.Millisecond,
		MaxBackoff:         500 * time.Millisecond,
		DefaultMaxAttempts: 3,
	}, queue.NewFileStore(filepath.Join(dir, "tasks.json")))
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	dedup, _ := queue.NewDedupStore(filepath.Join(dir, "dedup.json"))
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)

	// 消费者：3 个并发 worker
	w := queue.NewWorker(q, 3, queue.Idempotent(dedup, func(ctx context.Context, t *queue.Task) error {
		switch {
		case t.Payload == "poison":
			return queue.Permanent(errors.New("undecodable payload"))
		case rand.Intn(3) == 0:
			return errors.New("flaky downstream")
		default:
			return nil
		}
	}), logger)
	w.Start()

	// 生产者：投递 12 个任务，其中 2 个毒消息
	for i := 0; i < 12; i++ {
		payload := "ok"
		if i%6 == 5 {
			payload = "poison"
		}
		err := q.Enqueue(&queue.Task{ID: fmt.Sprintf("job-%02d", i), Payload: payload})
		if err != nil {
			logger.Printf("enqueue job-%02d rejected: %v", i, err)
		}
	}
	// 演示重复提交被拒绝
	if err := q.Enqueue(&queue.Task{ID: "job-00"}); err != nil {
		logger.Printf("duplicate enqueue rejected: %v", err)
	}

	// 等待所有任务到达终态
	for {
		stats := q.Stats()
		inflight := stats[queue.StatusPending] + stats[queue.StatusLeased]
		if inflight == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	w.Stop()

	stats := q.Stats()
	logger.Printf("final stats: %v", stats)
	for i := 0; i < 12; i++ {
		if t, ok := q.Get(fmt.Sprintf("job-%02d", i)); ok && t.Status == queue.StatusDead {
			logger.Printf("dead letter %s after %d attempts: %s", t.ID, t.Attempts, t.LastError)
		}
	}
}
