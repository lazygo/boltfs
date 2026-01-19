package boltfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero"
	bolt "go.etcd.io/bbolt"
)

// BoltFS 基于BoltDB的文件系统，实现afero.Fs接口
type BoltFS struct {
	db       *bolt.DB
	dbPath   string
	readOnly bool
	closed   bool
	mu       sync.RWMutex

	// 缓存
	metadataCache map[string]*FileMetadata
	dirCache      map[string][]*FileMetadata
	cacheMutex    sync.RWMutex

	// 文件级读写锁（带引用计数）
	fileLocks     map[string]*FileLock
	fileLockMutex sync.RWMutex
}

// 错误定义
var (
	ErrIsDirectory  = errors.New("is a directory")
	ErrNotDirectory = errors.New("not a directory")
)

// NewBoltFS 创建新的BoltDB文件系统
func NewBoltFS(dbPath string, readOnly bool) (*BoltFS, error) {
	opts := &bolt.Options{
		Timeout:  5 * time.Second,
		ReadOnly: readOnly,
	}

	db, err := bolt.Open(dbPath, 0644, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt db: %w", err)
	}

	fs := &BoltFS{
		db:            db,
		dbPath:        dbPath,
		readOnly:      readOnly,
		metadataCache: make(map[string]*FileMetadata),
		dirCache:      make(map[string][]*FileMetadata),
		fileLocks:     make(map[string]*FileLock),
	}

	if !readOnly {
		if err := fs.initializeDB(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to initialize db: %w", err)
		}
	}

	return fs, nil
}

func (fs *BoltFS) acquireFileLock(path string, write bool) *FileLock {
	cleanPath := fs.normalizePath(path)

	fs.fileLockMutex.Lock()
	defer fs.fileLockMutex.Unlock()

	// 检查是否已存在
	lock, exists := fs.fileLocks[cleanPath]
	if !exists {
		lock = newFileLock(cleanPath, fs)
		fs.fileLocks[cleanPath] = lock
	}

	// 根据模式获取锁
	if write {
		lock.acquireWrite()
	} else {
		lock.acquireRead()
	}

	return lock
}

// releaseFileLock 释放文件锁
func (fs *BoltFS) releaseFileLock(lock *FileLock, write bool) {
	needCleanup := false

	if write {
		needCleanup = lock.releaseWrite()
	} else {
		needCleanup = lock.releaseRead()
	}

	// 如果引用计数为0，清理锁
	if needCleanup {
		fs.fileLockMutex.Lock()
		defer fs.fileLockMutex.Unlock()

		// 再次检查引用计数（避免竞态条件）
		if lock.getRefCount() == 0 {
			delete(fs.fileLocks, lock.path)
		}
	}
}

// initializeDB 初始化数据库结构
func (fs *BoltFS) initializeDB() error {
	return fs.db.Update(func(tx *bolt.Tx) error {
		// 创建必要的buckets
		buckets := [][]byte{
			[]byte("metadata"),
			[]byte("data"),
			[]byte("dir_index"),
			[]byte("thumbnails"), // 缩略图专用
		}

		for _, bucketName := range buckets {
			if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
			}
		}

		// 创建根目录
		rootMeta := NewFileMetadata("/", true, 0755|os.ModeDir)
		rootMeta.ParentDir = ""

		metadataBucket := tx.Bucket([]byte("metadata"))
		data, err := rootMeta.Marshal()
		if err != nil {
			return err
		}

		return metadataBucket.Put([]byte("/"), data)
	})
}

// Close 关闭文件系统
func (fs *BoltFS) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.closed {
		return nil
	}

	fs.closed = true

	// 清理缓存 - 创建新的空 map，而不是设为 nil
	fs.cacheMutex.Lock()
	fs.metadataCache = make(map[string]*FileMetadata)
	fs.dirCache = make(map[string][]*FileMetadata)
	fs.cacheMutex.Unlock()

	// 清理文件锁
	fs.fileLockMutex.Lock()
	fs.fileLocks = make(map[string]*FileLock)
	fs.fileLockMutex.Unlock()

	return fs.db.Close()
}

// Name 返回文件系统名称
func (fs *BoltFS) Name() string {
	return "BoltFS"
}

// Open 打开文件（只读）
func (fs *BoltFS) Open(name string) (afero.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

// OpenFile 打开文件
func (fs *BoltFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if name == "" {
		return nil, &os.PathError{Op: "open", Path: "", Err: os.ErrInvalid}
	}

	if err := fs.checkClosed(); err != nil {
		return nil, err
	}

	cleanName := fs.normalizePath(name)

	// 判断是否需要写权限
	isWriteMode := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0

	// 只读文件系统检查
	if fs.readOnly && isWriteMode {
		return nil, errors.New("read-only file system")
	}

	// 获取文件锁
	var fileLock *FileLock
	if isWriteMode {
		fileLock = fs.acquireFileLock(cleanName, true) // 写锁
	} else {
		fileLock = fs.acquireFileLock(cleanName, false) // 读锁
	}

	// 检查文件是否存在
	meta, err := fs.getMetadata(cleanName)
	if err != nil && err != os.ErrNotExist {
		// 获取元数据失败，释放锁
		fs.releaseFileLock(fileLock, isWriteMode)
		return nil, err
	}

	// 文件不存在
	if meta == nil {
		if flag&os.O_CREATE == 0 {
			fs.releaseFileLock(fileLock, isWriteMode)
			return nil, os.ErrNotExist
		}

		// 创建新文件
		file, err := fs.createFileWithLock(cleanName, flag, perm, fileLock, isWriteMode)
		if err != nil {
			fs.releaseFileLock(fileLock, isWriteMode)
			return nil, err
		}
		return file, nil
	}

	// 文件存在
	if meta.IsDir {
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			fs.releaseFileLock(fileLock, isWriteMode)
			return nil, os.ErrExist
		}

		// 目录文件：如果需要写入，确保是写锁
		if isWriteMode && fileLock.getRefCount() == 1 {
			// 尝试安全升级锁
			if fileLock.safeUpgradeLock() {
				isWriteMode = true // 升级成功
			} else {
				// 升级失败，释放读锁，重新获取写锁
				fs.releaseFileLock(fileLock, false)
				fileLock = fs.acquireFileLock(cleanName, true)
				isWriteMode = true
			}
		}

		file := newBoltFile(fs, cleanName, meta, flag)
		file.fileLock = fileLock
		file.isWriteLocked = isWriteMode
		file.lockAcquired = true
		return file, nil
	}

	// 普通文件
	if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		fs.releaseFileLock(fileLock, isWriteMode)
		return nil, os.ErrExist
	}

	// 如果需要清空文件
	if flag&os.O_TRUNC != 0 {
		meta.Size = 0
		meta.DataKey = ""
		meta.ModTime = time.Now()
		meta.Version++

		// 需要确保是写锁
		if !isWriteMode {
			// 尝试安全升级锁
			if fileLock.getRefCount() == 1 && fileLock.safeUpgradeLock() {
				isWriteMode = true
			} else {
				// 升级失败，释放读锁，重新获取写锁
				fs.releaseFileLock(fileLock, false)
				fileLock = fs.acquireFileLock(cleanName, true)
				isWriteMode = true
			}
		}

		err := fs.db.Update(func(tx *bolt.Tx) error {
			return fs.saveMetadata(tx, meta)
		})
		if err != nil {
			fs.releaseFileLock(fileLock, isWriteMode)
			return nil, err
		}
	}

	file := newBoltFile(fs, cleanName, meta, flag)
	file.fileLock = fileLock
	file.isWriteLocked = isWriteMode
	file.lockAcquired = true

	// 追加模式
	if flag&os.O_APPEND != 0 {
		file.readOffset = meta.Size
	}

	return file, nil
}

// createFile 创建新文件
func (fs *BoltFS) createFile(name string, flag int, perm os.FileMode) (*BoltFile, error) {
	return fs.createFileWithLock(name, flag, perm, nil, false)
}

// createFileWithLock 创建新文件（带锁管理）
func (fs *BoltFS) createFileWithLock(name string, flag int, perm os.FileMode, fileLock *FileLock, isWriteLocked bool) (*BoltFile, error) {
	cleanName := fs.normalizePath(name)
	parentDir := filepath.Dir(cleanName)

	// 确保父目录存在
	if parentDir != "/" && parentDir != "." {
		parentMeta, err := fs.getMetadata(parentDir)
		if err != nil || !parentMeta.IsDir {
			// 创建父目录
			// 注意：这里可能需要获取父目录的锁，简化处理
			if err := fs.MkdirAll(parentDir, 0755); err != nil {
				return nil, err
			}
		}
	}

	// 创建文件元数据
	meta := NewFileMetadata(cleanName, false, perm)
	meta.ParentDir = parentDir
	meta.ModTime = time.Now()
	meta.AccessTime = time.Now()

	// 在一个事务中保存元数据和更新目录索引
	err := fs.db.Update(func(tx *bolt.Tx) error {
		// 保存元数据
		if err := fs.saveMetadata(tx, meta); err != nil {
			return err
		}

		// 更新目录索引
		return fs.updateDirIndexTx(tx, parentDir, cleanName, false)
	})

	if err != nil {
		return nil, err
	}

	file := newBoltFile(fs, cleanName, meta, flag)
	file.fileLock = fileLock
	file.isWriteLocked = isWriteLocked
	file.lockAcquired = true
	return file, nil
}

// Create 创建文件
func (fs *BoltFS) Create(name string) (afero.File, error) {
	if err := fs.checkClosed(); err != nil {
		return nil, err
	}
	return fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// Mkdir 创建目录
func (fs *BoltFS) Mkdir(name string, perm os.FileMode) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	cleanName := fs.normalizePath(name)

	// 检查是否已存在
	if _, err := fs.getMetadata(cleanName); err == nil {
		return os.ErrExist
	}

	parentDir := filepath.Dir(cleanName)

	// 确保父目录存在
	if parentDir != "/" && parentDir != "." {
		parentMeta, err := fs.getMetadata(parentDir)
		if err != nil || !parentMeta.IsDir {
			return os.ErrNotExist
		}
	}

	// 创建目录元数据
	meta := NewFileMetadata(cleanName, true, perm|os.ModeDir)
	meta.ParentDir = parentDir

	// 在一个事务中保存元数据和更新目录索引
	return fs.db.Update(func(tx *bolt.Tx) error {
		// 保存目录元数据
		if err := fs.saveMetadata(tx, meta); err != nil {
			return err
		}
		// 更新目录索引
		return fs.updateDirIndexTx(tx, parentDir, cleanName, true)
	})
}

// MkdirAll 递归创建目录
func (fs *BoltFS) MkdirAll(path string, perm os.FileMode) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	cleanPath := fs.normalizePath(path)

	if cleanPath == "/" {
		return nil
	}

	// 分割路径
	parts := strings.Split(strings.Trim(cleanPath, "/"), "/")
	currentPath := ""

	for i, part := range parts {
		if part == "" {
			continue
		}

		if currentPath == "" {
			currentPath = "/" + part
		} else {
			currentPath = currentPath + "/" + part
		}

		// 检查当前路径是否存在
		meta, err := fs.getMetadata(currentPath)
		if err == nil {
			// 存在但不是目录
			if !meta.IsDir && i < len(parts)-1 {
				return ErrNotDirectory
			}
			continue
		}

		// 创建目录
		mode := perm
		if i < len(parts)-1 {
			mode = 0755 // 中间目录使用默认权限
		}

		if err := fs.Mkdir(currentPath, mode|os.ModeDir); err != nil && err != os.ErrExist {
			return err
		}
	}

	return nil
}

// Remove 删除文件或空目录
func (fs *BoltFS) Remove(name string) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	cleanName := fs.normalizePath(name)

	meta, err := fs.getMetadata(cleanName)
	if err != nil {
		return err
	}

	// 如果是目录，检查是否为空
	if meta.IsDir {
		entries, err := fs.listDirectory(cleanName)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return errors.New("directory not empty")
		}
	}

	// 从父目录索引中移除
	if err := fs.removeFromDirIndex(meta.ParentDir, cleanName); err != nil {
		return err
	}

	// 删除元数据
	if err := fs.deleteMetadata(cleanName); err != nil {
		return err
	}

	// 如果是文件，删除数据
	if !meta.IsDir && meta.DataKey != "" {
		return fs.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("data"))
			if bucket != nil {
				return bucket.Delete([]byte(meta.DataKey))
			}
			return nil
		})
	}

	return nil
}

// RemoveAll 递归删除
func (fs *BoltFS) RemoveAll(path string) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	cleanPath := fs.normalizePath(path)

	return fs.db.Update(func(tx *bolt.Tx) error {
		return fs.removeAllRecursive(tx, cleanPath)
	})
}

// removeAllRecursive 递归删除
func (fs *BoltFS) removeAllRecursive(tx *bolt.Tx, path string) error {
	// 获取元数据
	metadataBucket := tx.Bucket([]byte("metadata"))
	data := metadataBucket.Get([]byte(path))
	if data == nil {
		return nil
	}

	var meta FileMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}

	// 如果是目录，递归删除子项
	if meta.IsDir {
		// 获取目录索引
		dirIndexBucket := tx.Bucket([]byte("dir_index"))
		dirData := dirIndexBucket.Get([]byte(path))

		if dirData != nil {
			var entries []string
			if err := json.Unmarshal(dirData, &entries); err != nil {
				return err
			}

			for _, entry := range entries {
				childPath := path + "/" + entry
				if path == "/" {
					childPath = "/" + entry
				}

				if err := fs.removeAllRecursive(tx, childPath); err != nil {
					return err
				}
			}
		}
	}

	// 从父目录索引中移除
	if path != "/" {
		if err := fs.removeFromDirIndexTx(tx, meta.ParentDir, path); err != nil {
			return err
		}
	}

	// 删除元数据
	if err := metadataBucket.Delete([]byte(path)); err != nil {
		return err
	}

	// 清除缓存
	fs.cacheMutex.Lock()
	delete(fs.metadataCache, path)
	delete(fs.dirCache, path)
	fs.cacheMutex.Unlock()

	// 如果是文件，删除数据
	if !meta.IsDir && meta.DataKey != "" {
		dataBucket := tx.Bucket([]byte("data"))
		if dataBucket != nil {
			dataBucket.Delete([]byte(meta.DataKey))
		}
	}

	return nil
}

// Rename 重命名文件或目录
func (fs *BoltFS) Rename(oldname, newname string) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	oldPath := fs.normalizePath(oldname)
	newPath := fs.normalizePath(newname)

	if oldPath == newPath {
		return nil
	}

	// 获取源文件和目标文件的写锁
	// 注意：需要按固定顺序获取锁以避免死锁
	paths := []string{oldPath, newPath}
	sort.Strings(paths)

	var locks []*FileLock
	defer func() {
		// 确保释放所有锁
		for _, lock := range locks {
			fs.releaseFileLock(lock, true)
		}
	}()

	// 获取所有需要的锁
	for _, path := range paths {
		lock := fs.acquireFileLock(path, true)
		locks = append(locks, lock)
	}

	return fs.db.Update(func(tx *bolt.Tx) error {
		return fs.renameInternal(tx, oldPath, newPath)
	})
}

// renameInternal 内部重命名实现
func (fs *BoltFS) renameInternal(tx *bolt.Tx, oldPath, newPath string) error {
	// 获取旧文件元数据
	metadataBucket := tx.Bucket([]byte("metadata"))
	if metadataBucket == nil {
		return errors.New("metadata bucket not found")
	}

	oldData := metadataBucket.Get([]byte(oldPath))
	if oldData == nil {
		return os.ErrNotExist
	}

	var oldMeta FileMetadata
	if err := json.Unmarshal(oldData, &oldMeta); err != nil {
		return err
	}

	// 检查新路径是否已存在
	if metadataBucket.Get([]byte(newPath)) != nil {
		return os.ErrExist
	}

	// 确保新路径的父目录存在
	newParent := filepath.Dir(newPath)
	if newParent != "/" && newParent != "." {
		parentData := metadataBucket.Get([]byte(newParent))
		if parentData == nil {
			return os.ErrNotExist
		}
		var parentMeta FileMetadata
		if err := json.Unmarshal(parentData, &parentMeta); err != nil {
			return err
		}
		if !parentMeta.IsDir {
			return ErrNotDirectory
		}
	}

	// 创建新元数据，避免复制互斥锁；保持 oldMeta 不变用于后续数据迁移
	newMeta := oldMeta.Clone()
	newMeta.Name = newPath
	newMeta.ParentDir = newParent
	newMeta.ModTime = time.Now()

	// 如果是文件，更新DataKey
	if !oldMeta.IsDir {
		newMeta.DataKey = fs.generateDataKey(newPath)
	}

	// 保存新元数据
	newData, err := newMeta.Marshal()
	if err != nil {
		return err
	}
	if err := metadataBucket.Put([]byte(newPath), newData); err != nil {
		return err
	}

	// 从旧父目录索引中移除旧文件
	if err := fs.removeFromDirIndexTx(tx, oldMeta.ParentDir, oldPath); err != nil {
		return err
	}

	// 添加到新父目录索引
	if err := fs.updateDirIndexTx(tx, newParent, newPath, oldMeta.IsDir); err != nil {
		return err
	}

	// 如果是目录，需要递归处理子项
	if oldMeta.IsDir {
		// 先更新子项的路径
		if err := fs.renameDirectoryContents(tx, oldPath, newPath); err != nil {
			return err
		}
	}

	// 删除旧元数据（必须在更新子项后）
	if err := metadataBucket.Delete([]byte(oldPath)); err != nil {
		return err
	}

	// 如果是文件，复制数据并删除旧数据
	if !oldMeta.IsDir && oldMeta.DataKey != "" {
		dataBucket := tx.Bucket([]byte("data"))
		if dataBucket != nil {
			oldData := dataBucket.Get([]byte(oldMeta.DataKey))
			if oldData != nil {
				if err := dataBucket.Put([]byte(newMeta.DataKey), oldData); err != nil {
					return err
				}
				dataBucket.Delete([]byte(oldMeta.DataKey))
			}
		}
	}

	// 清除缓存
	fs.cacheMutex.Lock()
	delete(fs.metadataCache, oldPath)
	delete(fs.dirCache, oldPath)
	if oldMeta.ParentDir != newParent {
		// 如果父目录改变，清除两个父目录的缓存
		delete(fs.dirCache, oldMeta.ParentDir)
		delete(fs.dirCache, newParent)
	}
	fs.cacheMutex.Unlock()

	return nil
}

// renameDirectoryContents 重命名目录内容
func (fs *BoltFS) renameDirectoryContents(tx *bolt.Tx, oldDir, newDir string) error {
	// 获取旧目录的索引
	dirIndexBucket := tx.Bucket([]byte("dir_index"))
	oldIndexData := dirIndexBucket.Get([]byte(oldDir))
	if oldIndexData == nil {
		return nil
	}

	var entries []string
	if err := json.Unmarshal(oldIndexData, &entries); err != nil {
		return err
	}

	for _, entry := range entries {
		oldPath := oldDir + "/" + entry
		if oldDir == "/" {
			oldPath = "/" + entry
		}

		newPath := newDir + "/" + entry
		if newDir == "/" {
			newPath = "/" + entry
		}

		if err := fs.renameInternal(tx, oldPath, newPath); err != nil {
			return err
		}
	}

	return nil
}

// Stat 获取文件信息
func (fs *BoltFS) Stat(name string) (os.FileInfo, error) {
	cleanName := fs.normalizePath(name)

	meta, err := fs.getMetadata(cleanName)
	if err != nil {
		return nil, err
	}

	return meta.ToFileInfo(), nil
}

// Chmod 修改文件权限
func (fs *BoltFS) Chmod(name string, mode os.FileMode) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	cleanName := fs.normalizePath(name)

	meta, err := fs.getMetadata(cleanName)
	if err != nil {
		return err
	}

	// 保留目录标志
	if meta.IsDir {
		mode = mode | os.ModeDir
	}

	meta.Mode = uint32(mode)
	meta.ModTime = time.Now()

	return fs.db.Update(func(tx *bolt.Tx) error {
		meta.Mode = uint32(mode)
		meta.ModTime = time.Now()
		return fs.saveMetadata(tx, meta)
	})
}

// Chtimes 修改文件时间
func (fs *BoltFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	cleanName := fs.normalizePath(name)

	meta, err := fs.getMetadata(cleanName)
	if err != nil {
		return err
	}

	meta.AccessTime = atime
	meta.ModTime = mtime

	return fs.db.Update(func(tx *bolt.Tx) error {
		meta.AccessTime = atime
		meta.ModTime = mtime
		return fs.saveMetadata(tx, meta)
	})
}

// 辅助方法

func (fs *BoltFS) normalizePath(path string) string {
	if path == "" || path == "." {
		return "/"
	}

	// 清理路径
	clean := filepath.Clean(path)

	// 确保以/开头
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}

	// 处理根目录
	if clean == "//" {
		return "/"
	}

	return clean
}

func (fs *BoltFS) getMetadata(name string) (*FileMetadata, error) {
	if err := fs.checkClosed(); err != nil {
		return nil, err
	}

	cleanName := fs.normalizePath(name)

	// 检查缓存
	fs.cacheMutex.RLock()

	// 检查缓存是否已初始化
	if fs.metadataCache == nil {
		fs.cacheMutex.RUnlock()
		// 文件系统可能已关闭，或者缓存未初始化
		return nil, errors.New("file system cache not available")
	}

	if meta, ok := fs.metadataCache[cleanName]; ok {
		fs.cacheMutex.RUnlock()
		return meta, nil
	}
	fs.cacheMutex.RUnlock()

	var meta *FileMetadata
	err := fs.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("metadata"))
		if bucket == nil {
			return errors.New("metadata bucket not found")
		}

		data := bucket.Get([]byte(cleanName))
		if data == nil {
			return os.ErrNotExist
		}

		meta = &FileMetadata{}
		return json.Unmarshal(data, meta)
	})

	if err != nil {
		return nil, err
	}

	// 更新缓存
	fs.cacheMutex.Lock()
	fs.metadataCache[cleanName] = meta
	fs.cacheMutex.Unlock()

	return meta, nil
}

// SaveMetadata 方法，用于外部调用
func (fs *BoltFS) SaveMetadata(meta *FileMetadata) error {
	return fs.db.Update(func(tx *bolt.Tx) error {
		return fs.saveMetadata(tx, meta)
	})
}

func (fs *BoltFS) saveMetadata(tx *bolt.Tx, meta *FileMetadata) error {
	bucket := tx.Bucket([]byte("metadata"))
	if bucket == nil {
		return errors.New("metadata bucket not found")
	}

	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	err = bucket.Put([]byte(meta.Name), data)
	if err == nil {
		// 更新缓存
		fs.cacheMutex.Lock()
		fs.metadataCache[meta.Name] = meta
		fs.cacheMutex.Unlock()
	}

	return err
}

func (fs *BoltFS) deleteMetadata(name string) error {
	cleanName := fs.normalizePath(name)

	err := fs.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("metadata"))
		if bucket == nil {
			return errors.New("metadata bucket not found")
		}
		return bucket.Delete([]byte(cleanName))
	})

	if err == nil {
		// 清除缓存
		fs.cacheMutex.Lock()
		delete(fs.metadataCache, cleanName)
		delete(fs.dirCache, cleanName)
		fs.cacheMutex.Unlock()
	}

	return err
}

func (fs *BoltFS) generateDataKey(name string) string {
	return fmt.Sprintf("data:%s:%d", name, time.Now().UnixNano())
}

func (fs *BoltFS) updateDirIndexTx(tx *bolt.Tx, dirPath, entryName string, isDir bool) error {
	cleanDir := fs.normalizePath(dirPath)
	if cleanDir == "" {
		cleanDir = "/"
	}

	bucket := tx.Bucket([]byte("dir_index"))
	if bucket == nil {
		return errors.New("dir_index bucket not found")
	}

	// 获取当前目录的索引
	var entries []string
	data := bucket.Get([]byte(cleanDir))

	if data != nil {
		if err := json.Unmarshal(data, &entries); err != nil {
			return err
		}
	}

	// 检查是否已存在
	entryNameOnly := filepath.Base(entryName) // 只存储文件名，不存储完整路径
	for _, entry := range entries {
		if entry == entryNameOnly {
			return nil // 已存在
		}
	}

	// 添加新条目（只存储文件名部分）
	entries = append(entries, entryNameOnly)

	newData, err := json.Marshal(entries)
	if err != nil {
		return err
	}

	return bucket.Put([]byte(cleanDir), newData)
}

func (fs *BoltFS) removeFromDirIndex(dirPath, entryName string) error {
	return fs.db.Update(func(tx *bolt.Tx) error {
		return fs.removeFromDirIndexTx(tx, dirPath, entryName)
	})
}

func (fs *BoltFS) removeFromDirIndexTx(tx *bolt.Tx, dirPath, entryName string) error {
	cleanDir := fs.normalizePath(dirPath)
	if cleanDir == "" {
		cleanDir = "/"
	}

	bucket := tx.Bucket([]byte("dir_index"))
	if bucket == nil {
		return errors.New("dir_index bucket not found")
	}

	data := bucket.Get([]byte(cleanDir))
	if data == nil {
		return nil
	}

	var entries []string
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}

	// 获取要删除的条目的基本名称
	entryBase := filepath.Base(entryName)

	// 移除条目
	newEntries := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry != entryBase {
			newEntries = append(newEntries, entry)
		}
	}

	if len(newEntries) == 0 {
		return bucket.Delete([]byte(cleanDir))
	}

	newData, err := json.Marshal(newEntries)
	if err != nil {
		return err
	}

	return bucket.Put([]byte(cleanDir), newData)
}

func (fs *BoltFS) listDirectory(dirPath string) ([]*FileMetadata, error) {
	cleanDir := fs.normalizePath(dirPath)

	// 检查缓存
	fs.cacheMutex.RLock()
	if entries, ok := fs.dirCache[cleanDir]; ok {
		fs.cacheMutex.RUnlock()
		return entries, nil
	}
	fs.cacheMutex.RUnlock()

	var entries []string
	err := fs.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("dir_index"))
		if bucket == nil {
			return nil
		}

		data := bucket.Get([]byte(cleanDir))
		if data == nil {
			return nil
		}

		return json.Unmarshal(data, &entries)
	})

	if err != nil {
		return nil, err
	}

	// 获取每个条目的元数据
	metas := make([]*FileMetadata, 0, len(entries))
	for _, entry := range entries {
		fullPath := cleanDir + "/" + entry
		if cleanDir == "/" {
			fullPath = "/" + entry
		}

		meta, err := fs.getMetadata(fullPath)
		if err == nil {
			metas = append(metas, meta)
		}
	}

	// 更新缓存
	fs.cacheMutex.Lock()
	fs.dirCache[cleanDir] = metas
	fs.cacheMutex.Unlock()

	return metas, nil
}

// 针对缩略图的专用方法

// StoreThumbnail 存储缩略图
func (fs *BoltFS) StoreThumbnail(originalPath string, thumbnailData []byte, contentType string) error {
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	cleanPath := fs.normalizePath(originalPath)

	// 获取原文件元数据
	meta, err := fs.getMetadata(cleanPath)
	if err != nil {
		return err
	}

	// 生成缩略图key
	thumbKey := fmt.Sprintf("thumb:%s:%d", cleanPath, time.Now().UnixNano())

	// 保存缩略图数据
	err = fs.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("thumbnails"))
		if bucket == nil {
			return errors.New("thumbnails bucket not found")
		}
		return bucket.Put([]byte(thumbKey), thumbnailData)
	})

	if err != nil {
		return err
	}

	// 更新元数据
	return fs.db.Update(func(tx *bolt.Tx) error {
		meta.ThumbnailKey = thumbKey
		meta.ContentType = contentType
		return fs.saveMetadata(tx, meta)
	})
}

// GetThumbnail 获取缩略图
func (fs *BoltFS) GetThumbnail(path string) ([]byte, error) {
	cleanPath := fs.normalizePath(path)

	meta, err := fs.getMetadata(cleanPath)
	if err != nil {
		return nil, err
	}

	if meta.ThumbnailKey == "" {
		return nil, os.ErrNotExist
	}

	var thumbnail []byte
	err = fs.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("thumbnails"))
		if bucket == nil {
			return errors.New("thumbnails bucket not found")
		}

		data := bucket.Get([]byte(meta.ThumbnailKey))
		if data == nil {
			return os.ErrNotExist
		}

		thumbnail = make([]byte, len(data))
		copy(thumbnail, data)
		return nil
	})

	return thumbnail, err
}

// 统计信息

// Stats 获取文件系统统计信息
func (fs *BoltFS) Stats() (map[string]any, error) {
	if err := fs.checkClosed(); err != nil {
		return nil, err
	}
	stats := make(map[string]any)

	err := fs.db.View(func(tx *bolt.Tx) error {
		// 统计文件数量
		metadataBucket := tx.Bucket([]byte("metadata"))
		if metadataBucket == nil {
			return errors.New("metadata bucket not found")
		}

		fileCount := 0
		dirCount := 0
		totalSize := int64(0)
		files := make([]map[string]any, 0)
		directories := make([]map[string]any, 0)

		cursor := metadataBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var meta FileMetadata
			if err := json.Unmarshal(v, &meta); err != nil {
				continue
			}

			if meta.IsDir {
				directories = append(directories, map[string]any{"name": meta.Name, "size": meta.Size, "mod_time": meta.ModTime})
			} else {
				fileCount++
				totalSize += meta.Size
				files = append(files, map[string]any{"name": meta.Name, "size": meta.Size, "mod_time": meta.ModTime})
			}
		}

		stats["files"] = files
		stats["file_count"] = fileCount
		stats["directories"] = directories
		stats["dir_count"] = dirCount
		stats["total_size"] = totalSize
		stats["db_size"] = fs.db.Stats().TxStats.PageCount * int64(tx.DB().Info().PageSize)

		return nil
	})

	return stats, err
}

// Chown 修改文件所有者（在Windows上不支持，在Unix-like系统上模拟）
func (fs *BoltFS) Chown(name string, uid, gid int) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	// 我们的虚拟文件系统不支持Unix风格的权限所有者
	// 为了兼容afero接口，我们忽略这个操作
	return nil
}

// Lchown 修改符号链接所有者（不支持）
func (fs *BoltFS) Lchown(name string, uid, gid int) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	return nil
}

// SymlinkIfPossible 创建符号链接（不支持）
func (fs *BoltFS) SymlinkIfPossible(oldname, newname string) error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	return errors.New("symbolic links not supported")
}

// ReadlinkIfPossible 读取符号链接（不支持）
func (fs *BoltFS) ReadlinkIfPossible(name string) (string, error) {
	if err := fs.checkClosed(); err != nil {
		return "", err
	}
	return "", errors.New("symbolic links not supported")
}

// Compact 压缩数据库
func (fs *BoltFS) Compact() error {
	if err := fs.checkClosed(); err != nil {
		return err
	}
	if fs.readOnly {
		return errors.New("read-only file system")
	}

	// BoltDB会自动处理碎片，这里可以添加一些清理逻辑
	return fs.db.Update(func(tx *bolt.Tx) error {
		// 可以在这里清理过期数据
		return nil
	})
}

// 在所有需要数据库操作的方法开始时检查关闭状态
func (fs *BoltFS) checkClosed() error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.closed {
		return errors.New("file system is closed")
	}
	return nil
}
