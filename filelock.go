package boltfs

import (
	"sync"
	"sync/atomic"
	"time"
)

// FileLock 带引用计数的文件锁
type FileLock struct {
	*sync.RWMutex
	refCount  int32      // 引用计数（原子操作）
	path      string     // 锁对应的路径
	fs        *BoltFS    // 指向文件系统，用于清理
	upgrading int32      // 标记是否正在升级（原子操作）
	cond      *sync.Cond // 用于升级的条件变量
}

func newFileLock(path string, fs *BoltFS) *FileLock {
	lock := &FileLock{
		RWMutex:   &sync.RWMutex{},
		refCount:  0,
		path:      path,
		fs:        fs,
		upgrading: 0,
	}
	lock.cond = sync.NewCond(lock.RWMutex) // 使用RWMutex作为条件变量的Locker
	return lock
}

// acquireRead 获取读锁并增加引用计数
func (fl *FileLock) acquireRead() {
	// 等待升级完成
	for atomic.LoadInt32(&fl.upgrading) == 1 {
		fl.cond.Wait()
	}

	fl.RLock()
	atomic.AddInt32(&fl.refCount, 1)
}

// acquireWrite 获取写锁并增加引用计数
func (fl *FileLock) acquireWrite() {
	fl.Lock()
	atomic.AddInt32(&fl.refCount, 1)
}

// releaseRead 释放读锁并减少引用计数
func (fl *FileLock) releaseRead() bool {
	fl.RUnlock()
	newCount := atomic.AddInt32(&fl.refCount, -1)

	// 如果升级正在进行且没有其他读锁，唤醒等待的升级操作
	if atomic.LoadInt32(&fl.upgrading) == 1 && newCount == 1 {
		fl.cond.Broadcast()
	}

	return newCount == 0
}

// releaseWrite 释放写锁并减少引用计数
func (fl *FileLock) releaseWrite() bool {
	fl.Unlock()
	newCount := atomic.AddInt32(&fl.refCount, -1)
	return newCount == 0
}

// safeUpgradeLock 安全的锁升级
func (fl *FileLock) safeUpgradeLock() bool {
	// 尝试标记升级开始
	if !atomic.CompareAndSwapInt32(&fl.upgrading, 0, 1) {
		return false // 已经有其他协程在升级
	}
	defer atomic.StoreInt32(&fl.upgrading, 0)

	// 检查是否只有一个读锁持有者
	currentCount := atomic.LoadInt32(&fl.refCount)
	if currentCount != 1 {
		return false // 不是唯一的持有者，不能升级
	}

	// 现在我们持有唯一的读锁，可以尝试升级
	// 注意：RWMutex不允许直接升级，需要释放读锁再获取写锁
	fl.RUnlock()
	atomic.AddInt32(&fl.refCount, -1) // 因为我们释放了读锁，减少计数

	// 获取写锁
	fl.Lock()
	atomic.AddInt32(&fl.refCount, 1) // 增加写锁的计数

	// 检查在获取写锁期间是否有其他协程获取了读锁
	if atomic.LoadInt32(&fl.refCount) > 1 {
		// 有新的读锁，不能升级，回退到读锁
		fl.Unlock()
		atomic.AddInt32(&fl.refCount, -1)

		fl.RLock()
		atomic.AddInt32(&fl.refCount, 1)
		return false
	}

	return true
}

// getRefCount 获取当前引用计数
func (fl *FileLock) getRefCount() int32 {
	return atomic.LoadInt32(&fl.refCount)
}

// 调试和监控相关方法

// LockStatus 锁状态信息
type LockStatus struct {
	Path      string    `json:"path"`
	RefCount  int32     `json:"ref_count"`
	LockType  string    `json:"lock_type"` // "read", "write", "none"
	HeldSince time.Time `json:"held_since,omitempty"`
}

// GetLockStatus 获取所有文件锁的状态
func (fs *BoltFS) GetLockStatus() []LockStatus {
	fs.fileLockMutex.RLock()
	defer fs.fileLockMutex.RUnlock()

	var status []LockStatus
	for path, lock := range fs.fileLocks {
		count := lock.getRefCount()
		if count > 0 {
			status = append(status, LockStatus{
				Path:     path,
				RefCount: count,
				LockType: "active",
			})
		}
	}

	return status
}

// IsPathLocked 检查路径是否被锁定
func (fs *BoltFS) IsPathLocked(path string) bool {
	cleanPath := fs.normalizePath(path)

	fs.fileLockMutex.RLock()
	defer fs.fileLockMutex.RUnlock()

	lock, exists := fs.fileLocks[cleanPath]
	if !exists {
		return false
	}

	return lock.getRefCount() > 0
}
