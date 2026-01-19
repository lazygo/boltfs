package boltfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// IndexManager 索引管理器
type IndexManager struct {
	db *bolt.DB

	// Bucket名称
	pathIndexBucket    []byte // 路径->元数据索引
	dirIndexBucket     []byte // 目录->内容列表索引
	contentIndexBucket []byte // 内容哈希->文件列表（用于去重）

	// 内存缓存
	pathCache    map[string]*FileMetadata
	dirCache     map[string][]string
	cacheMutex   sync.RWMutex
	cacheEnabled bool
}

// NewIndexManager 创建索引管理器
func NewIndexManager(db *bolt.DB, enableCache bool) *IndexManager {
	return &IndexManager{
		db:                 db,
		pathIndexBucket:    []byte("path_index"),
		dirIndexBucket:     []byte("dir_index"),
		contentIndexBucket: []byte("content_index"),
		pathCache:          make(map[string]*FileMetadata),
		dirCache:           make(map[string][]string),
		cacheEnabled:       enableCache,
	}
}

// Initialize 初始化索引Buckets
func (im *IndexManager) Initialize() error {
	return im.db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{
			im.pathIndexBucket,
			im.dirIndexBucket,
			im.contentIndexBucket,
		}

		for _, bucket := range buckets {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	})
}

// ==================== 路径索引管理 ====================

// PutPath 添加/更新路径索引
func (im *IndexManager) PutPath(path string, meta *FileMetadata) error {
	cleanPath := normalizePath(path)

	// 序列化元数据
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	err = im.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.pathIndexBucket)
		return bucket.Put([]byte(cleanPath), data)
	})

	if err == nil && im.cacheEnabled {
		im.cacheMutex.Lock()
		im.pathCache[cleanPath] = meta
		im.cacheMutex.Unlock()
	}

	return err
}

// GetPath 获取路径索引
func (im *IndexManager) GetPath(path string) (*FileMetadata, error) {
	cleanPath := normalizePath(path)

	// 检查缓存
	if im.cacheEnabled {
		im.cacheMutex.RLock()
		if meta, ok := im.pathCache[cleanPath]; ok {
			im.cacheMutex.RUnlock()
			return meta, nil
		}
		im.cacheMutex.RUnlock()
	}

	var meta *FileMetadata
	err := im.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.pathIndexBucket)
		data := bucket.Get([]byte(cleanPath))

		if data == nil {
			return os.ErrNotExist
		}

		return json.Unmarshal(data, &meta)
	})

	if err == nil && im.cacheEnabled && meta != nil {
		im.cacheMutex.Lock()
		im.pathCache[cleanPath] = meta
		im.cacheMutex.Unlock()
	}

	return meta, err
}

// DeletePath 删除路径索引
func (im *IndexManager) DeletePath(path string) error {
	cleanPath := normalizePath(path)

	err := im.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.pathIndexBucket)
		return bucket.Delete([]byte(cleanPath))
	})

	if err == nil && im.cacheEnabled {
		im.cacheMutex.Lock()
		delete(im.pathCache, cleanPath)
		im.cacheMutex.Unlock()
	}

	return err
}

// PathExists 检查路径是否存在
func (im *IndexManager) PathExists(path string) bool {
	cleanPath := normalizePath(path)

	if im.cacheEnabled {
		im.cacheMutex.RLock()
		_, exists := im.pathCache[cleanPath]
		im.cacheMutex.RUnlock()
		if exists {
			return true
		}
	}

	exists := false
	im.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.pathIndexBucket)
		data := bucket.Get([]byte(cleanPath))
		exists = data != nil
		return nil
	})

	return exists
}

// ==================== 目录索引管理 ====================

// AddToDirIndex 添加文件到目录索引
func (im *IndexManager) AddToDirIndex(dirPath, fileName string, isDir bool) error {
	cleanDir := normalizePath(dirPath)
	if cleanDir == "" {
		cleanDir = "/"
	}

	return im.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.dirIndexBucket)

		// 获取当前目录的条目列表
		var entries []DirEntry
		data := bucket.Get([]byte(cleanDir))

		if data != nil {
			json.Unmarshal(data, &entries)
		}

		// 检查是否已存在
		for _, entry := range entries {
			if entry.Name == fileName {
				// 更新现有条目
				entry.IsDir = isDir
				entry.Modified = time.Now()

				// 重新序列化并保存
				newData, _ := json.Marshal(entries)
				return bucket.Put([]byte(cleanDir), newData)
			}
		}

		// 添加新条目
		entries = append(entries, DirEntry{
			Name:     fileName,
			IsDir:    isDir,
			Modified: time.Now(),
		})

		// 排序
		sort.Slice(entries, func(i, j int) bool {
			// 目录在前，文件在后，按名称排序
			if entries[i].IsDir != entries[j].IsDir {
				return entries[i].IsDir
			}
			return entries[i].Name < entries[j].Name
		})

		// 保存
		newData, err := json.Marshal(entries)
		if err != nil {
			return err
		}

		// 更新缓存
		if im.cacheEnabled {
			im.cacheMutex.Lock()
			im.dirCache[cleanDir] = toStringSlice(entries)
			im.cacheMutex.Unlock()
		}

		return bucket.Put([]byte(cleanDir), newData)
	})
}

// RemoveFromDirIndex 从目录索引移除文件
func (im *IndexManager) RemoveFromDirIndex(dirPath, fileName string) error {
	cleanDir := normalizePath(dirPath)
	if cleanDir == "" {
		cleanDir = "/"
	}

	return im.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.dirIndexBucket)

		data := bucket.Get([]byte(cleanDir))
		if data == nil {
			return nil // 目录索引不存在
		}

		var entries []DirEntry
		if err := json.Unmarshal(data, &entries); err != nil {
			return err
		}

		// 移除指定条目
		newEntries := make([]DirEntry, 0, len(entries))
		for _, entry := range entries {
			if entry.Name != fileName {
				newEntries = append(newEntries, entry)
			}
		}

		// 如果目录为空，删除索引
		if len(newEntries) == 0 {
			bucket.Delete([]byte(cleanDir))

			if im.cacheEnabled {
				im.cacheMutex.Lock()
				delete(im.dirCache, cleanDir)
				im.cacheMutex.Unlock()
			}
			return nil
		}

		// 保存更新后的列表
		newData, err := json.Marshal(newEntries)
		if err != nil {
			return err
		}

		// 更新缓存
		if im.cacheEnabled {
			im.cacheMutex.Lock()
			im.dirCache[cleanDir] = toStringSlice(newEntries)
			im.cacheMutex.Unlock()
		}

		return bucket.Put([]byte(cleanDir), newData)
	})
}

// ListDir 列出目录内容
func (im *IndexManager) ListDir(dirPath string) ([]DirEntry, error) {
	cleanDir := normalizePath(dirPath)
	if cleanDir == "" {
		cleanDir = "/"
	}

	// 检查缓存
	if im.cacheEnabled {
		im.cacheMutex.RLock()
		if cached, ok := im.dirCache[cleanDir]; ok {
			im.cacheMutex.RUnlock()
			return toDirEntrySlice(cached), nil
		}
		im.cacheMutex.RUnlock()
	}

	var entries []DirEntry
	err := im.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.dirIndexBucket)
		data := bucket.Get([]byte(cleanDir))

		if data == nil {
			entries = []DirEntry{} // 空目录
			return nil
		}

		return json.Unmarshal(data, &entries)
	})

	if err == nil && im.cacheEnabled {
		im.cacheMutex.Lock()
		im.dirCache[cleanDir] = toStringSlice(entries)
		im.cacheMutex.Unlock()
	}

	return entries, err
}

// ==================== 内容索引（去重功能）====================

// AddContentIndex 添加内容索引（用于文件去重）
func (im *IndexManager) AddContentIndex(contentHash string, filePath string) error {
	return im.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.contentIndexBucket)

		// 获取当前哈希对应的文件列表
		var fileList []string
		data := bucket.Get([]byte(contentHash))

		if data != nil {
			json.Unmarshal(data, &fileList)
		}

		// 检查是否已存在
		for _, f := range fileList {
			if f == filePath {
				return nil // 已存在
			}
		}

		// 添加新文件路径
		fileList = append(fileList, filePath)

		newData, err := json.Marshal(fileList)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(contentHash), newData)
	})
}

// FindDuplicateFiles 查找重复文件
func (im *IndexManager) FindDuplicateFiles(contentHash string) ([]string, error) {
	var fileList []string

	err := im.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.contentIndexBucket)
		data := bucket.Get([]byte(contentHash))

		if data == nil {
			return nil
		}

		return json.Unmarshal(data, &fileList)
	})

	return fileList, err
}

// RemoveContentIndex 移除内容索引
func (im *IndexManager) RemoveContentIndex(contentHash, filePath string) error {
	return im.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.contentIndexBucket)

		data := bucket.Get([]byte(contentHash))
		if data == nil {
			return nil
		}

		var fileList []string
		if err := json.Unmarshal(data, &fileList); err != nil {
			return err
		}

		// 移除指定路径
		newList := make([]string, 0, len(fileList))
		for _, f := range fileList {
			if f != filePath {
				newList = append(newList, f)
			}
		}

		// 如果列表为空，删除整个条目
		if len(newList) == 0 {
			return bucket.Delete([]byte(contentHash))
		}

		newData, err := json.Marshal(newList)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(contentHash), newData)
	})
}

// ==================== 批量操作 ====================

// BatchUpdate 批量更新索引
func (im *IndexManager) BatchUpdate(updates []IndexUpdate) error {
	return im.db.Update(func(tx *bolt.Tx) error {
		pathBucket := tx.Bucket(im.pathIndexBucket)
		dirBucket := tx.Bucket(im.dirIndexBucket)

		for _, update := range updates {
			switch update.Type {
			case UpdateTypePath:
				// 更新路径索引
				data, _ := json.Marshal(update.Metadata)
				pathBucket.Put([]byte(update.Key), data)

			case UpdateTypeDirAdd:
				// 添加到目录索引
				im.batchAddToDir(dirBucket, update.DirPath, update.FileName, update.IsDir)

			case UpdateTypeDirRemove:
				// 从目录索引移除
				im.batchRemoveFromDir(dirBucket, update.DirPath, update.FileName)
			}
		}
		return nil
	})
}

func (im *IndexManager) batchAddToDir(bucket *bolt.Bucket, dirPath, fileName string, isDir bool) {
	var entries []DirEntry
	data := bucket.Get([]byte(dirPath))

	if data != nil {
		json.Unmarshal(data, &entries)
	}

	// 检查并添加
	exists := false
	for i, entry := range entries {
		if entry.Name == fileName {
			entries[i].IsDir = isDir
			entries[i].Modified = time.Now()
			exists = true
			break
		}
	}

	if !exists {
		entries = append(entries, DirEntry{
			Name:     fileName,
			IsDir:    isDir,
			Modified: time.Now(),
		})
	}

	// 排序并保存
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})

	newData, _ := json.Marshal(entries)
	bucket.Put([]byte(dirPath), newData)
}

func (im *IndexManager) batchRemoveFromDir(bucket *bolt.Bucket, dirPath, fileName string) {
	data := bucket.Get([]byte(dirPath))
	if data == nil {
		return
	}

	var entries []DirEntry
	json.Unmarshal(data, &entries)

	newEntries := make([]DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Name != fileName {
			newEntries = append(newEntries, entry)
		}
	}

	if len(newEntries) == 0 {
		bucket.Delete([]byte(dirPath))
	} else {
		newData, _ := json.Marshal(newEntries)
		bucket.Put([]byte(dirPath), newData)
	}
}

// ==================== 搜索功能 ====================

// SearchByPrefix 前缀搜索
func (im *IndexManager) SearchByPrefix(prefix string) ([]string, error) {
	cleanPrefix := normalizePath(prefix)
	var results []string

	err := im.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.pathIndexBucket)
		cursor := bucket.Cursor()

		// 使用前缀搜索
		for k, _ := cursor.Seek([]byte(cleanPrefix)); k != nil && strings.HasPrefix(string(k), cleanPrefix); k, _ = cursor.Next() {
			results = append(results, string(k))
		}

		return nil
	})

	return results, err
}

// SearchByPattern 模式搜索（简单通配符）
func (im *IndexManager) SearchByPattern(pattern string) ([]string, error) {
	var results []string

	err := im.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(im.pathIndexBucket)

		return bucket.ForEach(func(k, v []byte) error {
			key := string(k)

			// 简单通配符匹配：* 匹配任意字符
			if matchPattern(key, pattern) {
				results = append(results, key)
			}

			return nil
		})
	})

	return results, err
}

// ==================== 统计信息 ====================

// GetStats 获取索引统计信息
func (im *IndexManager) GetStats() (*IndexStats, error) {
	stats := &IndexStats{}

	err := im.db.View(func(tx *bolt.Tx) error {
		// 统计路径索引
		pathBucket := tx.Bucket(im.pathIndexBucket)
		if pathBucket != nil {
			stats.TotalFiles = 0
			pathBucket.ForEach(func(k, v []byte) error {
				stats.TotalFiles++
				return nil
			})
		}

		// 统计目录索引
		dirBucket := tx.Bucket(im.dirIndexBucket)
		if dirBucket != nil {
			stats.TotalDirs = 0
			dirBucket.ForEach(func(k, v []byte) error {
				stats.TotalDirs++
				return nil
			})
		}

		// 统计内容索引
		contentBucket := tx.Bucket(im.contentIndexBucket)
		if contentBucket != nil {
			stats.UniqueContentHashes = 0
			contentBucket.ForEach(func(k, v []byte) error {
				stats.UniqueContentHashes++
				return nil
			})
		}

		return nil
	})

	return stats, err
}

// ==================== 辅助类型和函数 ====================

// DirEntry 目录条目
type DirEntry struct {
	Name     string    `json:"name"`
	IsDir    bool      `json:"is_dir"`
	Modified time.Time `json:"modified"`
}

// IndexUpdate 索引更新操作
type IndexUpdate struct {
	Type     UpdateType
	Key      string
	Metadata *FileMetadata
	DirPath  string
	FileName string
	IsDir    bool
}

type UpdateType int

const (
	UpdateTypePath UpdateType = iota
	UpdateTypeDirAdd
	UpdateTypeDirRemove
)

// IndexStats 索引统计信息
type IndexStats struct {
	TotalFiles          int64 `json:"total_files"`
	TotalDirs           int64 `json:"total_dirs"`
	UniqueContentHashes int64 `json:"unique_content_hashes"`
}

func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return clean
}

func toStringSlice(entries []DirEntry) []string {
	result := make([]string, len(entries))
	for i, entry := range entries {
		result[i] = entry.Name
	}
	return result
}

func toDirEntrySlice(names []string) []DirEntry {
	result := make([]DirEntry, len(names))
	for i, name := range names {
		result[i] = DirEntry{
			Name:  name,
			IsDir: false, // 需要从其他来源获取此信息
		}
	}
	return result
}

func matchPattern(name, pattern string) bool {
	// 将文件系统通配符转换为正则表达式
	// * 匹配任意字符（包括路径分隔符）
	// ? 匹配单个字符
	// 其他字符按字面匹配

	// 转义正则特殊字符
	pattern = regexp.QuoteMeta(pattern)

	// 将通配符转换为正则表达式
	pattern = strings.ReplaceAll(pattern, "\\*", ".*")
	pattern = strings.ReplaceAll(pattern, "\\?", ".")

	// 添加锚点
	pattern = "^" + pattern + "$"

	matched, err := regexp.MatchString(pattern, name)
	if err != nil {
		return false
	}

	return matched
}
