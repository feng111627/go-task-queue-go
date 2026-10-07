package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store 负责任务状态的本地持久化，进程重启后可恢复。
type Store interface {
	// Save 全量持久化当前任务快照。
	Save(tasks map[string]*Task) error
	// Load 读取持久化的任务快照；无历史数据时返回空 map。
	Load() (map[string]*Task, error)
	// Close 释放资源。
	Close() error
}

// FileStore 以 JSON 文件持久化任务状态。
// 写入采用「临时文件 + fsync + 原子重命名」，避免崩溃留下半截文件。
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore 创建基于 path 的文件存储。path 为空时退化为纯内存存储（不落盘）。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Save(tasks map[string]*Task) error {
	if s.path == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := make(map[string]Task, len(tasks))
	for id, t := range tasks {
		snap[id] = *t
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("queue: marshal tasks: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("queue: mkdir for store: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("queue: open tmp store: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("queue: write tmp store: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("queue: fsync store: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("queue: close store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("queue: rename store: %w", err)
	}
	return nil
}

func (s *FileStore) Load() (map[string]*Task, error) {
	tasks := map[string]*Task{}
	if s.path == "" {
		return tasks, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return tasks, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read store: %w", err)
	}
	var snap map[string]Task
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("queue: parse store %s: %w", s.path, err)
	}
	for id, t := range snap {
		task := t
		tasks[id] = &task
	}
	return tasks, nil
}

func (s *FileStore) Close() error { return nil }
