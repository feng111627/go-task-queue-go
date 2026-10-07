# go-task-queue-go

进程内本地异步任务队列。用进程内的生产者 / 消费者模拟真实投递，重点保证三件事在并发、失败、
进程重启下都有**确定、可预期**的结果：

1. 同一个任务同一时刻只会被一个消费者领取；
2. 失败任务按规则重试、超上限进死信，且失败原因不丢失；
3. 任务状态持久化到本地文件，重启后未完成任务恢复到确定状态；重复投递不产生重复副作用。

## 快速开始

```go
q, _ := taskqueue.New(taskqueue.Config{
    Capacity:    100,
    Concurrency: 4,
    MaxAttempts: 3,
    StoreFile:   "data/tasks.db",
})
defer q.Close()

q.Start(ctx, func(ctx context.Context, t *taskqueue.Task) error {
    // 处理 t.Payload ...
    return nil // nil=成功；taskqueue.Retryable(err)=可重试；taskqueue.Terminal(err)=不可重试
})

id, err := q.Enqueue(ctx, "业务幂等键", "任务类型", payload)
```

可运行演示：`go run ./cmd/demo data/demo.db`（先删除旧 db 可看全新流程；保留 db 再次运行可看
重启后的去重与终态恢复）。

## 投递语义

- **至少一次（at-least-once）投递**：任务失败或进程在处理中崩溃后会被重新投递，因此同一个
  任务的处理器**可能被执行多次**。队列通过领取互斥保证"同一时刻只有一个消费者在执行某任务"，
  但不保证处理器只执行一次——恰好一次需要业务处理器自己做幂等（见下文）。
- **领取互斥**：消费者领取动作在队列锁内串行完成，任务状态由 `pending` 原子翻转为 `running`，
  并记录 `Attempt` 与租约到期时间 `LeaseUntil`；其他消费者无法重复领取同一任务。
- **FIFO 近似**：多个到期任务按 `CreatedAt` 最早者优先领取。

### 入队拒绝（绝不静默丢弃）

| 情况 | 返回 | 调用方含义 |
| --- | --- | --- |
| 未终结任务数已达 `Capacity` | `ErrQueueFull` | 队列已满，稍后重试或扩容 |
| 相同 `IdemKey` 已提交过 | `ErrDuplicate`（同时返回原任务 ID） | 重复提交，可据此查询原任务 |
| 队列已 `Close` | `ErrClosed` | 进程收尾阶段，拒绝新任务 |

判重优先于容量检查：即使队列已满，重复提交也会明确返回 `ErrDuplicate` 而不是被误判为满载。

## 重试与死信规则

任务每次被领取 `Attempt++`。处理器返回后：

- 返回 `nil`：`succeeded`（终态），清空失败原因。
- 返回 `taskqueue.Terminal(err)`（不可重试错误）：**首次即进入死信**，不再重试。
- 返回 `taskqueue.Retryable(err)` 或未包装的普通 error（默认可重试，避免业务忘包装时失败被吞）：
  - 若 `Attempt < MaxAttempts`：回到 `pending`，等待退避后重新投递；
  - 若 `Attempt >= MaxAttempts`：进入死信。
- 处理器 `panic`：自动恢复并按可重试错误处理。

退避时间（第 `attempt` 次失败后）：

```
min(BaseBackoff * 2^(attempt-1), MaxBackoff)
```

死信任务（`dead`）为终态：保留 `LastError`（最后一次失败的原始错误信息）、`Attempt` 等全部
信息，不会自动消失，可通过 `Queue.Get` / 持久化文件审计后人工或外部补偿。

## 重启恢复

每次状态翻转（创建 / 领取 / 成功 / 失败 / 入死信）都立即以"临时文件 + fsync + 原子 rename +
目录 fsync"的方式整体快照落盘。`New` 打开已有文件时：

- `pending` 任务：保持 `pending`，其 `AvailableAt`（退避到期时间）保留，到期后继续投递；
- `running` 任务（上次未正常处理完，进程被杀/宕机）：重置为 `pending`、清空租约并立即重新
  投递，**已消耗的 `Attempt` 保留**（继续计入上限，不会无限重试）。

`succeeded` / `dead` 终态任务不恢复、不再投递，但记录保留用于幂等判重与审计。

## 幂等约定（重复投递为何不会有重复副作用）

队列提供两层去重，职责不同：

1. **提交去重（队列负责）**：`Enqueue` 传相同的非空 `IdemKey`，只接受第一次，后续返回
   `ErrDuplicate` 和原任务 ID。适用于上游超时重试、点击重复提交等。
2. **处理幂等（业务负责，必须做）**：因为投递语义是至少一次（失败重试、`running` 崩溃恢复都
   会重投），处理器必须对外部副作用做幂等。推荐以 `Task.ID`（或业务流水号）为幂等键，先做
   "副作用是否已发生"的检查 / 唯一约束插入，再执行业务：

```go
handler := func(ctx context.Context, t *taskqueue.Task) error {
    if ledger.Exists(t.ID) { return nil } // 重复投递：副作用已做过，直接确认成功
    doExternalEffect(t)
    ledger.Mark(t.ID)                     // 最好与外部副作用在同一事务内
    return nil
}
```

`idempotency_test.go` 演示了"副作用已发生但任务确认失败 → 重试再投"时，副作用只执行一次。

## 配置项（taskqueue.Config）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Capacity` | 100 | 容量上限，按未终结任务（pending+running）计，满载入队返回 `ErrQueueFull` |
| `MaxAttempts` | 3 | 最大处理次数（含首次），达到后进死信 |
| `BaseBackoff` | 1s | 指数退避基准：`BaseBackoff*2^(n-1)` |
| `MaxBackoff` | 30s | 退避间隔上限 |
| `Concurrency` | 1 | 消费者 goroutine 数 |
| `Lease` | 30s | 处理租约；用于崩溃恢复判定（进程内靠锁互斥，不会提前抢占） |
| `StoreFile` | `data/tasks.db` | 本地持久化文件路径 |

## 存储与局限

`FileStore` 是面向单机 / 单进程的 JSON 整体快照存储（每次写全量刷盘），适合本地模拟与轻量
使用；任务量很大时应替换 `Store` 接口为 SQLite / 数据库等实现。队列不支持跨进程、多实例并发
访问同一文件。

## 测试

```
go test -race -count=1 ./... -v
```

覆盖场景与日志（每条用例均以 `[场景]/[用例]/[结果]` 打印）：

| 场景 | 测试 | 断言要点 |
| --- | --- | --- |
| 并发领取 | `TestConcurrentClaim` | 8 消费者 × 40 任务：领取互斥、无重复处理、峰值并发=8 |
| 容量拒绝 | `TestEnqueueReject` | 满载返回 `ErrQueueFull`，原因可区分 |
| 重复投递 | `TestEnqueueReject` / `TestDuplicateSubmitNoDoubleSideEffect` | 同幂等键 `ErrDuplicate`+原 ID；重投多次副作用仅 1 次 |
| 重试到死信 | `TestRetryToDeadLetter` | 处理次数=MaxAttempts、退避指数递增并封顶、死信保留原始原因 |
| 不可重试 | `TestTerminalError` | `Terminal` 首发即死信 |
| 重试后成功 | `TestRetryThenSuccess` | 退避期间恢复则正常成功终结 |
| 重启恢复 | `TestRestartPendingKept` / `TestRestartRunningRedelivered` | pending 到期续投；running 重置立即重投且尝试次数保留 |
