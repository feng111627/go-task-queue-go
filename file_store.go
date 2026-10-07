package taskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// snapshot 是磁盘文件的整体序列化结构。
type snapshot struct {
	Tasks []*Task `json:"tasks"`
}

// FileStore 是基于本地文件的任务存储。
// 内存中保存全量任务（map 加锁），每次写操作都生成完整快照，
// 通过 "临时文件 + fsync + rename + 目录 fsync" 原子替换，
// 保证进程崩溃后文件要么是旧版本要么是新版本，不会出现半写状态。
type FileStore struct {
	mu    sync.Mutex
	path  string
	tasks map[string]*Task
}

// NewFileStore 打开（不存在则创建）存储文件，并立即把已有内容载入内存。
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		path = filepath.Join("data", "tasks.db")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &FileStore{path: path, tasks: map[string]*Task{}}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileStore) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // 全新文件，内存为空
		}
		return err
	}
	if len(b) == 0 {
		return nil
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return err
	}
	for _, t := range snap.Tasks {
		s.tasks[t.ID] = t
	}
	return nil
}

// persist 在持锁状态下把全量任务原子刷盘。
func (s *FileStore) persist() error {
	snap := snapshot{Tasks: make([]*Task, 0, len(s.tasks))}
	for _, t := range s.tasks {
		snap.Tasks = append(snap.Tasks, t)
	}
	b, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".tasks-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后 Remove 是无害的 no-op
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil { // 文件内容落盘
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil { // 原子替换
		return err
	}
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync() // 保证 rename 元数据落盘
}

func clone(t *Task) *Task { c := *t; return &c }

func (s *FileStore) CreateTask(_ context.Context, t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[t.ID]; ok {
		return ErrDuplicate
	}
	for _, ex := range s.tasks {
		if t.IdemKey != "" && ex.IdemKey == t.IdemKey {
			return ErrDuplicate
		}
	}
	s.tasks[t.ID] = clone(t)
	return s.persist()
}

func (s *FileStore) GetTask(_ context.Context, id string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrUnknownTask
	}
	return clone(t), nil
}

func (s *FileStore) GetByIdemKey(_ context.Context, key string) (*Task, error) {
	if key == "" {
		return nil, ErrUnknownTask
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		if t.IdemKey == key {
			return clone(t), nil
		}
	}
	return nil, ErrUnknownTask
}

func (s *FileStore) UpdateTask(_ context.Context, t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[t.ID]; !ok {
		return ErrUnknownTask
	}
	s.tasks[t.ID] = clone(t)
	return s.persist()
}

func (s *FileStore) ListRecoverable(_ context.Context) ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Task
	for _, t := range s.tasks {
		if t.Status == StatusPending || t.Status == StatusRunning {
			out = append(out, clone(t))
		}
	}
	return out, nil
}

func (s *FileStore) CountActive(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.tasks {
		if t.Status == StatusPending || t.Status == StatusRunning {
			n++
		}
	}
	return n, nil
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil // 每次写操作已即时刷盘，这里无需额外动作
}
