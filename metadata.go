package boltfs

import (
	"encoding/json"
	"os"
	"path"
	"sync"
	"time"
)

// FileMetadata 文件元数据
type FileMetadata struct {
	Name         string    `json:"name"`
	Size         int64     `json:"size"`
	Mode         uint32    `json:"mode"`
	ModTime      time.Time `json:"mod_time"`
	AccessTime   time.Time `json:"access_time"`
	IsDir        bool      `json:"is_dir"`
	ParentDir    string    `json:"parent_dir"`
	DataKey      string    `json:"data_key"`      // 数据在BoltDB中的key
	Version      int       `json:"version"`       // 乐观锁版本
	ContentType  string    `json:"content_type"`  // 文件类型，对缩略图有用
	ThumbnailKey string    `json:"thumbnail_key"` // 缩略图key（如果有）

	// 添加互斥锁保护并发访问
	mu sync.RWMutex
}

// Lock 获取写锁
func (m *FileMetadata) Lock() {
	m.mu.Lock()
}

// Unlock 释放写锁
func (m *FileMetadata) Unlock() {
	m.mu.Unlock()
}

// RLock 获取读锁
func (m *FileMetadata) RLock() {
	m.mu.RLock()
}

// RUnlock 释放读锁
func (m *FileMetadata) RUnlock() {
	m.mu.RUnlock()
}

func (m *FileMetadata) Clone() *FileMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &FileMetadata{
		Name:         m.Name,
		Size:         m.Size,
		Mode:         m.Mode,
		ModTime:      m.ModTime,
		AccessTime:   m.AccessTime,
		IsDir:        m.IsDir,
		ParentDir:    m.ParentDir,
		DataKey:      m.DataKey,
		Version:      m.Version,
		ContentType:  m.ContentType,
		ThumbnailKey: m.ThumbnailKey,
	}
}

// 修改 GetSize 等需要并发安全的方法
func (m *FileMetadata) GetSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Size
}

func (m *FileMetadata) SetSize(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Size = size
}

// BoltFileInfo 实现os.FileInfo接口
type BoltFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *BoltFileInfo) Name() string       { return fi.name }
func (fi *BoltFileInfo) Size() int64        { return fi.size }
func (fi *BoltFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *BoltFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *BoltFileInfo) IsDir() bool        { return fi.isDir }
func (fi *BoltFileInfo) Sys() interface{}   { return nil }

// NewFileMetadata 创建新文件元数据
func NewFileMetadata(name string, isDir bool, perm os.FileMode) *FileMetadata {
	now := time.Now()
	return &FileMetadata{
		Name:       name,
		Size:       0,
		Mode:       uint32(perm),
		ModTime:    now,
		AccessTime: now,
		IsDir:      isDir,
		ParentDir:  "",
		DataKey:    "",
		Version:    1,
	}
}

// ToFileInfo 转换为os.FileInfo
// ToFileInfo 转换为os.FileInfo
func (m *FileMetadata) ToFileInfo() os.FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 获取文件名（最后一部分）
	name := m.Name
	if name != "/" {
		// 使用 path.Base 而不是 filepath.Base，因为我们使用 / 作为分隔符
		name = path.Base(name)
		if name == "" {
			name = "/"
		}
	}

	return &BoltFileInfo{
		name:    name,
		size:    m.Size,
		mode:    os.FileMode(m.Mode),
		modTime: m.ModTime,
		isDir:   m.IsDir,
	}
}

// Marshal 序列化元数据
func (m *FileMetadata) Marshal() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(m)
}

// Unmarshal 反序列化元数据
func (m *FileMetadata) Unmarshal(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Unmarshal(data, m)
}
