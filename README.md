# go-task-queue-go

进程内本地异步任务队列：生产者/消费者模型，支持并发处理、失败重试、死信与本地持久化恢复。

## 快速开始

```go
q, _ := queue.New(queue.Config{
    Capacity:           1024,
    BaseBackoff:        100 * time.Millisecond,
    MaxBackoff:         5 * time.Second,
    DefaultMaxAttempts: 3,
}, queue.NewFileStore("data/tasks.json"))

dedup, _ := queue.NewDedupStore("data/dedup.json")
w := queue.NewWorker(q, 4, queue.Idempotent(dedup, func(ctx context.Context, t *queue.Task) error {
    // 业务处理；返回 error 触发重试，queue.Permanent(err) 直接进死信
    return nil
}), nil)
w.Start()
defer w.Stop()

err := q.Enqueue(&queue.Task{ID: "job-1", Payload: "..."})
```

运行演示：`go run ./cmd/demo`；运行测试：`go test ./... -v`（日志中可见各场景与用例结果）。

## 投递语义

- **at-least-once**：任务被消费者领取后记为 `leased`，`Ack` 成功落盘后才算完成。
  进程崩溃时未确认的 `leased` 任务会在重启后重置为 `pending` 重新投递，因此**同一任务可能被投递多次**。
- **容量上限**：`pending + leased` 总数达到 `Capacity` 后，`Enqueue` 返回 `ErrQueueFull` 明确拒绝，不静默丢弃。
- **重复提交识别**：相同 `ID` 的任务（无论处于何种状态）再次入队返回 `ErrDuplicateTask`。
- **领取互斥**：`Next` 在锁内完成状态迁移，同一任务同一时刻只会被一个消费者领取。

## 重试与死信规则

- Handler 返回普通 `error` → 可重试：任务置回 `pending`，按**指数退避**安排下次可领取时间
  （`BaseBackoff * 2^(attempts-1)`，封顶 `MaxBackoff`）。
- Handler 返回 `queue.Permanent(err)` 包装的错误 → **不可重试**，直接进死信。
- 执行次数达到 `MaxAttempts`（默认取 `DefaultMaxAttempts`，可按任务覆盖）→ 进死信。
- 死信任务保留 `LastError`（原始失败原因）、`Attempts` 等完整现场，可通过 `Get`/`Stats` 观测，不会被吞掉。

## 幂等约定

- 队列保证 at-least-once，**消费端必须幂等**。
- 提供 `queue.Idempotent(dedup, handler)` 包装器：基于持久化的 `DedupStore` 登记已处理任务 ID，
  重复投递（重试、重启恢复、重复提交）时副作用只执行一次；登记发生在 `Ack` 之前。
- 状态变化（入队/领取/Ack/Nack）均即时落盘（临时文件 + fsync + 原子重命名），重启后状态确定。

## 可配置项（queue.Config）

| 字段 | 含义 | 默认值 |
|---|---|---|
| `Capacity` | 队列容量上限（pending+leased），满载拒绝入队 | 1024 |
| `BaseBackoff` | 首次重试间隔，之后指数递增 | 50ms |
| `MaxBackoff` | 重试间隔上限 | 5s |
| `DefaultMaxAttempts` | 任务默认最大执行次数（含首次） | 3 |

## 测试覆盖

`go test ./... -v`，关键场景：

1. **并发领取与容量**：多消费者并发处理，每个任务恰好被领取一次；满载返回 `ErrQueueFull`；重复提交返回 `ErrDuplicateTask`。
2. **失败重试到死信**：可重试错误按递增间隔重试至上限进死信并保留原因；永久错误一次即死信。
3. **重启恢复**：`leased` 任务重启后重置为 `pending`，全部恢复任务被消费完成。
4. **重复投递幂等**：重复提交被识别；重启引起的重复投递副作用仅执行一次。
