package boltfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrInvalidSeek = errors.New("invalid seek")
)

// BoltFile 实现afero.File接口
type BoltFile struct {
	fs         *BoltFS
	name       string
	metadata   *FileMetadata
	flag       int
	readOffset int64
	writeBuf   *bytes.Buffer // 写入缓冲区
	dirty      bool
	closed     bool
	mu         sync.RWMutex
}

// newBoltFile 创建新文件对象
func newBoltFile(fs *BoltFS, name string, meta *FileMetadata, flag int) *BoltFile {
	return &BoltFile{
		fs:         fs,
		name:       name,
		metadata:   meta,
		flag:       flag,
		readOffset: 0,
		writeBuf:   bytes.NewBuffer(nil),
		dirty:      false,
		closed:     false,
	}
}

// Read 读取文件
func (f *BoltFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// 检查文件系统是否已关闭
	if err := f.checkFSClosed(); err != nil {
		return 0, err
	}

	if f.metadata.IsDir {
		return 0, errors.New("is a directory")
	}

	// 如果有未保存的写入，先刷新
	if f.dirty && f.writeBuf.Len() > 0 {
		if err := f.flush(); err != nil {
			return 0, err
		}
	}

	// 从存储中读取数据
	var data []byte
	err = f.fs.db.View(func(tx *bolt.Tx) error {
		if f.metadata.DataKey == "" {
			return nil
		}
		bucket := tx.Bucket([]byte("data"))
		if bucket == nil {
			return nil
		}
		dbData := bucket.Get([]byte(f.metadata.DataKey))
		if dbData != nil {
			data = make([]byte, len(dbData))
			copy(data, dbData)
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	if data == nil || f.readOffset >= int64(len(data)) {
		return 0, io.EOF
	}

	// 计算可读取的数据量
	available := int64(len(data)) - f.readOffset
	toRead := int64(len(p))
	if toRead > available {
		toRead = available
	}

	// 复制数据
	copy(p, data[f.readOffset:f.readOffset+toRead])
	n = int(toRead)
	f.readOffset += toRead

	// 更新访问时间
	f.metadata.Lock()
	f.metadata.AccessTime = time.Now()
	f.metadata.Unlock()
	f.dirty = true

	return n, nil
}

// ReadAt 从指定位置读取
func (f *BoltFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// 检查文件系统是否已关闭
	if err := f.checkFSClosed(); err != nil {
		return 0, err
	}

	if f.metadata.IsDir {
		return 0, errors.New("is a directory")
	}

	if off < 0 {
		return 0, ErrInvalidSeek
	}

	// 如果有未保存的写入，先刷新
	if f.dirty && f.writeBuf.Len() > 0 {
		if err := f.flush(); err != nil {
			return 0, err
		}
	}

	// 从存储中读取数据
	var data []byte
	err = f.fs.db.View(func(tx *bolt.Tx) error {
		if f.metadata.DataKey == "" {
			return nil
		}
		bucket := tx.Bucket([]byte("data"))
		if bucket == nil {
			return nil
		}
		data = bucket.Get([]byte(f.metadata.DataKey))
		return nil
	})

	if err != nil {
		return 0, err
	}

	if data == nil || off >= int64(len(data)) {
		return 0, io.EOF
	}

	// 计算可读取的数据量
	available := int64(len(data)) - off
	toRead := int64(len(p))
	if toRead > available {
		toRead = available
		err = io.EOF
	}

	// 复制数据
	copy(p, data[off:off+toRead])
	n = int(toRead)

	// 更新访问时间
	f.metadata.AccessTime = time.Now()
	f.dirty = true

	return n, err
}

// Write 写入文件
func (f *BoltFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// 检查文件系统是否已关闭
	if err := f.checkFSClosed(); err != nil {
		return 0, err
	}

	if f.fs.readOnly {
		return 0, os.ErrPermission
	}

	if f.metadata.IsDir {
		return 0, errors.New("is a directory")
	}

	// 如果是追加模式，移动到文件末尾
	if f.flag&os.O_APPEND != 0 {
		f.readOffset = f.metadata.Size
	}

	// 写入缓冲区
	n, err = f.writeBuf.Write(p)
	if err != nil {
		return n, err
	}

	f.dirty = true

	// 更新元数据 - 需要复制元数据以避免竞争
	f.metadata.mu.Lock()
	f.metadata.Size += int64(n)
	f.metadata.ModTime = time.Now()
	f.metadata.Version++
	f.metadata.mu.Unlock()

	return n, nil
}

// WriteAt 在指定位置写入
func (f *BoltFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// 检查文件系统是否已关闭
	if err := f.checkFSClosed(); err != nil {
		return 0, err
	}

	if f.fs.readOnly {
		return 0, os.ErrPermission
	}

	if f.metadata.IsDir {
		return 0, errors.New("is a directory")
	}

	if off < 0 {
		return 0, ErrInvalidSeek
	}

	// 如果有未保存的写入，先刷新
	if f.dirty && f.writeBuf.Len() > 0 {
		if err := f.flush(); err != nil {
			return 0, err
		}
	}

	// 读取当前数据
	var currentData []byte
	err = f.fs.db.View(func(tx *bolt.Tx) error {
		if f.metadata.DataKey == "" {
			return nil
		}
		bucket := tx.Bucket([]byte("data"))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(f.metadata.DataKey))
		if data != nil {
			currentData = make([]byte, len(data))
			copy(currentData, data)
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	// 确保有足够空间
	requiredSize := off + int64(len(p))
	if requiredSize > int64(len(currentData)) {
		newData := make([]byte, requiredSize)
		copy(newData, currentData)
		currentData = newData
	}

	// 写入数据
	copy(currentData[off:], p)

	// 创建新的缓冲区，包含完整数据
	f.writeBuf = bytes.NewBuffer(nil)
	if _, err := f.writeBuf.Write(currentData); err != nil {
		return 0, err
	}

	f.dirty = true

	// 更新元数据
	if requiredSize > f.metadata.Size {
		f.metadata.Size = requiredSize
	}
	f.metadata.ModTime = time.Now()
	f.metadata.Version++

	// 立即刷新到数据库
	if err := f.flush(); err != nil {
		return 0, err
	}

	return len(p), nil
}

// Seek 移动文件指针
func (f *BoltFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// 检查文件系统是否已关闭
	if err := f.checkFSClosed(); err != nil {
		return 0, err
	}

	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.readOffset + offset
	case io.SeekEnd:
		newOffset = f.metadata.Size + offset
	default:
		return 0, ErrInvalidSeek
	}

	if newOffset < 0 {
		return 0, ErrInvalidSeek
	}

	f.readOffset = newOffset
	return newOffset, nil
}

// Sync 同步到存储
func (f *BoltFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}

	// 检查文件系统是否已关闭
	if err := f.checkFSClosed(); err != nil {
		return err
	}

	return f.flush()
}

// flush 刷新缓冲区到存储
// 避免嵌套事务
func (f *BoltFile) flush() error {
	if !f.dirty || f.writeBuf == nil || f.writeBuf.Len() == 0 {
		return nil
	}

	// 复制数据以避免竞争
	data := make([]byte, f.writeBuf.Len())
	copy(data, f.writeBuf.Bytes())

	// 直接更新数据库
	err := f.fs.db.Update(func(tx *bolt.Tx) error {
		// 获取或创建数据 bucket
		dataBucket, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}

		// 生成或使用现有 DataKey
		if f.metadata.DataKey == "" {
			f.metadata.DataKey = f.fs.generateDataKey(f.name)
		}

		// 保存数据
		if err := dataBucket.Put([]byte(f.metadata.DataKey), data); err != nil {
			return err
		}

		// 更新元数据
		f.metadata.Lock()
		f.metadata.Size = int64(len(data))
		f.metadata.ModTime = time.Now()
		f.metadata.Unlock()

		// 直接保存元数据到 metadata bucket
		metaBucket, err := tx.CreateBucketIfNotExists([]byte("metadata"))
		if err != nil {
			return err
		}

		metaData, err := f.metadata.Marshal()
		if err != nil {
			return err
		}

		return metaBucket.Put([]byte(f.metadata.Name), metaData)
	})

	if err == nil {
		f.dirty = false
		f.writeBuf.Reset()

		// 更新缓存 - 添加安全检查
		f.fs.cacheMutex.Lock()

		// 检查缓存是否已初始化（避免操作 nil map）
		if f.fs.metadataCache != nil {
			f.fs.metadataCache[f.metadata.Name] = f.metadata
		}

		f.fs.cacheMutex.Unlock()
	}

	return err
}

// checkFSClosed 检查文件系统是否已关闭
func (f *BoltFile) checkFSClosed() error {
	f.fs.mu.RLock()
	closed := f.fs.closed
	f.fs.mu.RUnlock()

	if closed {
		return errors.New("file system is closed")
	}
	return nil
}

// Close 关闭文件
func (f *BoltFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}

	// 刷新未保存的数据
	if f.dirty {
		if err := f.flush(); err != nil {
			f.closed = true
			f.writeBuf = nil
			return err
		}
	}

	f.closed = true
	f.writeBuf = nil

	return nil
}

// Readdir 读取目录内容
func (f *BoltFile) Readdir(count int) ([]os.FileInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return nil, os.ErrClosed
	}

	if !f.metadata.IsDir {
		return nil, errors.New("not a directory")
	}

	// 列出目录内容
	entries, err := f.fs.listDirectory(f.name)
	if err != nil {
		return nil, err
	}

	// 如果没有条目，返回空切片
	if len(entries) == 0 {
		return []os.FileInfo{}, nil
	}

	// 转换为os.FileInfo
	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		if entry != nil {
			infos = append(infos, entry.ToFileInfo())
		}
	}

	// 如果count > 0，只返回指定数量的条目
	if count > 0 && count < len(infos) {
		return infos[:count], nil
	}

	return infos, nil
}

// Readdirnames 读取目录名称
func (f *BoltFile) Readdirnames(count int) ([]string, error) {
	infos, err := f.Readdir(count)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}

	return names, nil
}

// Stat 获取文件信息
func (f *BoltFile) Stat() (os.FileInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return nil, os.ErrClosed
	}

	return f.metadata.ToFileInfo(), nil
}

// WriteString 写入字符串
func (f *BoltFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// Name 返回文件名
func (f *BoltFile) Name() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.name
}

// Truncate 截断文件
func (f *BoltFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}

	// 检查文件系统是否已关闭
	if err := f.checkFSClosed(); err != nil {
		return err
	}

	if f.fs.readOnly {
		return os.ErrPermission
	}

	if f.metadata.IsDir {
		return errors.New("is a directory")
	}

	if size < 0 {
		return errors.New("negative size")
	}

	// 读取当前数据
	var currentData []byte
	err := f.fs.db.View(func(tx *bolt.Tx) error {
		if f.metadata.DataKey == "" {
			return nil
		}
		bucket := tx.Bucket([]byte("data"))
		if bucket == nil {
			return nil
		}
		currentData = bucket.Get([]byte(f.metadata.DataKey))
		return nil
	})

	if err != nil {
		return err
	}

	// 调整数据大小
	var newData []byte
	if size > int64(len(currentData)) {
		newData = make([]byte, size)
		copy(newData, currentData)
	} else {
		newData = currentData[:size]
	}

	// 更新缓冲区
	f.writeBuf = bytes.NewBuffer(newData)
	f.dirty = true

	// 更新元数据
	f.metadata.Size = size
	f.metadata.ModTime = time.Now()
	f.metadata.Version++

	// 调整读偏移量
	if f.readOffset > size {
		f.readOffset = size
	}

	return nil
}
