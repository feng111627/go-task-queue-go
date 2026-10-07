package taskqueue

import "context"

// Store 是任务状态的本地持久化接口。
// 所有写操作必须在返回前落盘（fsync），保证进程崩溃/重启后状态不丢。
type Store interface {
	// CreateTask 持久化一个新任务；ID 或幂等键冲突时返回 ErrDuplicate。
	CreateTask(ctx context.Context, t *Task) error
	// GetTask 按 ID 读取任务；不存在返回 ErrUnknownTask。
	GetTask(ctx context.Context, id string) (*Task, error)
	// GetByIdemKey 按幂等键读取任务；幂等键为空或不存在返回 ErrUnknownTask。
	GetByIdemKey(ctx context.Context, key string) (*Task, error)
	// UpdateTask 原子更新任务（状态、尝试次数、错误原因、可领取时间等）。
	UpdateTask(ctx context.Context, t *Task) error
	// ListRecoverable 返回重启时需要恢复的任务：
	// 所有 pending 任务以及上次未正常处理完的 running 任务。
	ListRecoverable(ctx context.Context) ([]*Task, error)
	// CountActive 返回未终结任务数（pending + running），用于容量控制。
	CountActive(ctx context.Context) (int, error)
	// Close 关闭存储（刷盘并释放资源）。
	Close() error
}
