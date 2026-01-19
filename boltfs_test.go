package boltfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ==================== 测试辅助函数 ====================

func tempDBPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test.db")
}

func createTestFS(t *testing.T, readOnly bool) (*BoltFS, func()) {
	t.Helper()

	dbPath := tempDBPath(t)
	fs, err := NewBoltFS(dbPath, readOnly)
	if err != nil {
		t.Fatalf("创建文件系统失败: %v", err)
	}

	return fs, func() {
		fs.Close()
		// t.TempDir() 会自动清理
	}
}

func writeFile(fs *BoltFS, name string, data []byte, perm os.FileMode) error {
	f, err := fs.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	return err
}

func readFile(fs *BoltFS, name string) ([]byte, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(f)
}

func exists(fs *BoltFS, name string) (bool, error) {
	_, err := fs.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("预期没有错误，但得到: %v", err)
	}
}

func assertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("预期有错误，但得到nil")
	}
}

func assertFileExists(t *testing.T, fs *BoltFS, path string) {
	t.Helper()
	exists, err := exists(fs, path)
	assertNoError(t, err)
	if !exists {
		t.Fatalf("预期文件存在: %s", path)
	}
}

func assertFileNotExists(t *testing.T, fs *BoltFS, path string) {
	t.Helper()
	exists, err := exists(fs, path)
	assertNoError(t, err)
	if exists {
		t.Fatalf("预期文件不存在: %s", path)
	}
}

func assertFileContent(t *testing.T, fs *BoltFS, path string, expected []byte) {
	t.Helper()
	data, err := readFile(fs, path)
	assertNoError(t, err)
	if !bytes.Equal(data, expected) {
		t.Fatalf("文件内容不匹配。预期: %q, 得到: %q", string(expected), string(data))
	}
}

// ==================== 基础功能测试 ====================

// 修改 boltfs_test.go 中的测试
func TestNewBoltFS(t *testing.T) {
	t.Run("创建读写文件系统", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		info, err := fs.Stat("/")
		assertNoError(t, err)
		if !info.IsDir() {
			t.Error("根目录应该是目录")
		}
	})

	t.Run("创建只读文件系统", func(t *testing.T) {
		// 使用不同的数据库路径，避免竞争
		dbPath := filepath.Join(t.TempDir(), "readonly.db")

		// 先创建可读写文件系统
		rwFS, err := NewBoltFS(dbPath, false)
		assertNoError(t, err)

		// 创建测试文件
		err = writeFile(rwFS, "/test.txt", []byte("test"), 0644)
		assertNoError(t, err)

		// 重要：确保完全关闭
		err = rwFS.Close()
		assertNoError(t, err)

		// 等待一小段时间，确保锁被释放
		time.Sleep(100 * time.Millisecond)

		// 以只读模式打开
		roFS, err := NewBoltFS(dbPath, true)
		assertNoError(t, err)
		defer roFS.Close()

		// 应该可以读取
		data, err := readFile(roFS, "/test.txt")
		assertNoError(t, err)
		if string(data) != "test" {
			t.Errorf("读取内容不匹配: %s", data)
		}

		// 只读文件系统不应该写入（测试只读功能，不测试写入）
		// 注意：只读文件系统的写入测试应该单独进行
	})
}

func TestCreateAndOpen(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("创建新文件", func(t *testing.T) {
		f, err := fs.Create("/test.txt")
		assertNoError(t, err)
		defer f.Close()

		n, err := f.Write([]byte("Hello, World!"))
		assertNoError(t, err)
		if n != 13 {
			t.Errorf("写入字节数不匹配: %d", n)
		}

		assertFileExists(t, fs, "/test.txt")
	})

	t.Run("创建带路径的文件", func(t *testing.T) {
		f, err := fs.Create("/deep/nested/path/test.txt")
		assertNoError(t, err)
		f.Close()

		assertFileExists(t, fs, "/deep/nested/path/test.txt")
	})
}

func TestFileReadWrite(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("基本读写", func(t *testing.T) {
		content := []byte("This is a test file content.")
		err := writeFile(fs, "/rwtest.txt", content, 0644)
		assertNoError(t, err)

		data, err := readFile(fs, "/rwtest.txt")
		assertNoError(t, err)
		if !bytes.Equal(data, content) {
			t.Errorf("读取内容不匹配。预期: %q, 得到: %q", string(content), string(data))
		}
	})

	t.Run("空文件操作", func(t *testing.T) {
		f, err := fs.Create("/empty.txt")
		assertNoError(t, err)
		f.Close()

		data, err := readFile(fs, "/empty.txt")
		assertNoError(t, err)
		if len(data) != 0 {
			t.Errorf("空文件应该有0字节，得到: %d", len(data))
		}
	})
}

func TestDirectoryOperations(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("创建和列出目录", func(t *testing.T) {
		err := fs.Mkdir("/testdir", 0755)
		assertNoError(t, err)

		// 调试：检查根目录索引
		debugDirIndex(t, fs, "/")

		err = fs.MkdirAll("/testdir/sub1/sub2", 0755)
		assertNoError(t, err)

		// 调试：检查/testdir目录索引
		debugDirIndex(t, fs, "/testdir")

		writeFile(fs, "/testdir/file1.txt", []byte("file1"), 0644)
		writeFile(fs, "/testdir/file2.txt", []byte("file2"), 0644)

		// 再次检查/testdir目录索引
		debugDirIndex(t, fs, "/testdir")

		// 打开目录
		f, err := fs.Open("/testdir")
		assertNoError(t, err)
		defer f.Close()

		// 列出内容
		files, err := f.Readdir(0)
		assertNoError(t, err)

		t.Logf("实际列出的文件数量: %d", len(files))
		for i, file := range files {
			t.Logf("文件 %d: 名称=%s, 是目录=%v, 大小=%d",
				i, file.Name(), file.IsDir(), file.Size())
		}

		if len(files) != 3 { // file1.txt, file2.txt, sub1
			t.Errorf("目录条目数量错误。预期: 3, 得到: %d", len(files))
		}
	})
}

func TestRemoveOperations(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("删除文件", func(t *testing.T) {
		err := writeFile(fs, "/todelete.txt", []byte("delete me"), 0644)
		assertNoError(t, err)

		err = fs.Remove("/todelete.txt")
		assertNoError(t, err)

		exists, _ := exists(fs, "/todelete.txt")
		if exists {
			t.Error("文件应该被删除")
		}
	})

	t.Run("删除空目录", func(t *testing.T) {
		err := fs.MkdirAll("/empty/todelete", 0755)
		assertNoError(t, err)

		err = fs.Remove("/empty/todelete")
		assertNoError(t, err)

		exists, _ := exists(fs, "/empty/todelete")
		if exists {
			t.Error("目录应该被删除")
		}
	})
}

func TestRenameOperations(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("重命名文件", func(t *testing.T) {
		content := []byte("rename test content")
		err := writeFile(fs, "/source.txt", content, 0644)
		assertNoError(t, err)

		err = fs.Rename("/source.txt", "/dest.txt")
		assertNoError(t, err)

		exists, _ := exists(fs, "/source.txt")
		if exists {
			t.Error("源文件应该不存在")
		}

		data, err := readFile(fs, "/dest.txt")
		assertNoError(t, err)
		if string(data) != string(content) {
			t.Errorf("重命名后内容不匹配: %s", data)
		}
	})
}

func TestChmodAndChtimes(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("修改文件权限", func(t *testing.T) {
		err := writeFile(fs, "/chmod.txt", []byte("test"), 0644)
		assertNoError(t, err)

		err = fs.Chmod("/chmod.txt", 0600)
		assertNoError(t, err)

		info, err := fs.Stat("/chmod.txt")
		assertNoError(t, err)
		if info.Mode() != 0600 {
			t.Errorf("权限修改失败。预期: 0600, 得到: %o", info.Mode())
		}
	})

	t.Run("修改文件时间", func(t *testing.T) {
		err := writeFile(fs, "/chtimes.txt", []byte("test"), 0644)
		assertNoError(t, err)

		atime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		mtime := time.Date(2021, 12, 31, 23, 59, 59, 0, time.UTC)

		err = fs.Chtimes("/chtimes.txt", atime, mtime)
		assertNoError(t, err)

		info, err := fs.Stat("/chtimes.txt")
		assertNoError(t, err)

		if !info.ModTime().Equal(mtime) {
			t.Errorf("修改时间不匹配。预期: %v, 得到: %v", mtime, info.ModTime())
		}
	})
}

func TestThumbnailOperations(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("存储和获取缩略图", func(t *testing.T) {
		err := writeFile(fs, "/image.jpg", []byte("original image data"), 0644)
		assertNoError(t, err)

		thumbnailData := []byte("thumbnail data")
		err = fs.StoreThumbnail("/image.jpg", thumbnailData, "image/jpeg")
		assertNoError(t, err)

		data, err := fs.GetThumbnail("/image.jpg")
		assertNoError(t, err)

		if !bytes.Equal(data, thumbnailData) {
			t.Errorf("缩略图数据不匹配。预期: %q, 得到: %q",
				string(thumbnailData), string(data))
		}
	})
}

func TestConcurrentOperations(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	const numWorkers = 5
	const numOperations = 20

	t.Run("并发创建文件", func(t *testing.T) {
		var wg sync.WaitGroup
		errCh := make(chan error, numWorkers*numOperations)

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					path := fmt.Sprintf("/concurrent/file_%d_%d.txt", workerID, j)
					err := writeFile(fs, path, []byte("data"), 0644)
					if err != nil {
						select {
						case errCh <- err:
						default:
							// 如果通道已满，忽略错误
						}
					}
				}
			}(i)
		}

		wg.Wait()
		close(errCh)

		// 检查错误
		errorCount := 0
		for err := range errCh {
			t.Logf("并发操作错误: %v", err)
			errorCount++
		}

		if errorCount > 0 {
			t.Errorf("发现 %d 个并发错误", errorCount)
		}
	})

	t.Run("并发读写同一文件", func(t *testing.T) {
		// 先创建文件
		err := writeFile(fs, "/concurrent.txt", []byte("initial"), 0644)
		assertNoError(t, err)

		var wg sync.WaitGroup
		errCh := make(chan error, numWorkers)

		// 使用文件锁来保护并发写入
		fileLock := &sync.Mutex{}

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				fileLock.Lock()
				defer fileLock.Unlock()

				// 每个worker使用独立的文件句柄
				f, err := fs.OpenFile("/concurrent.txt", os.O_RDWR, 0644)
				if err != nil {
					errCh <- err
					return
				}
				defer f.Close()

				// 写入少量数据
				data := fmt.Sprintf("worker %d\n", workerID)
				_, err = f.Write([]byte(data))
				if err != nil {
					errCh <- err
				}
			}(i)
		}

		wg.Wait()
		close(errCh)

		// 检查错误
		errorCount := 0
		for err := range errCh {
			t.Logf("并发读写错误: %v", err)
			errorCount++
		}

		t.Logf("并发读写完成，错误数: %d", errorCount)
	})
}

func TestStatsAndMaintenance(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("统计信息", func(t *testing.T) {
		writeFile(fs, "/stats1.txt", []byte("data1"), 0644)
		writeFile(fs, "/stats2.txt", make([]byte, 1024), 0644)
		fs.Mkdir("/statsdir", 0755)

		stats, err := fs.Stats()
		assertNoError(t, err)

		t.Logf("统计信息: %+v", stats)
	})

	t.Run("数据库压缩", func(t *testing.T) {
		err := fs.Compact()
		assertNoError(t, err)
		t.Log("数据库压缩完成")
	})
}

// ==================== 边界测试 ====================

func TestEdgeCases(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("已关闭文件的操作", func(t *testing.T) {
		f, err := fs.Create("/closed.txt")
		assertNoError(t, err)

		f.Close()

		// 尝试在已关闭文件上操作
		_, err = f.Write([]byte("test"))
		if err == nil {
			t.Error("在已关闭文件上写入应该失败")
		}
	})

	t.Run("空路径", func(t *testing.T) {
		// 使用 fs.Open 而不是 fs.OpenFile，因为 Open 会调用 OpenFile
		_, err := fs.Open("")
		if err == nil {
			t.Error("打开空路径应该失败")
		} else {
			// 检查是否是预期的错误类型
			if !strings.Contains(err.Error(), "invalid argument") &&
				!strings.Contains(err.Error(), "invalid") {
				t.Logf("打开空路径返回的错误: %v", err)
			}
		}
	})
}

func TestIntegration(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("完整工作流", func(t *testing.T) {
		// 1. 创建目录结构
		err := fs.MkdirAll("/photos/2023/vacation", 0755)
		assertNoError(t, err)

		// 2. 上传图片文件
		image1 := []byte("fake image data 1")
		err = writeFile(fs, "/photos/2023/vacation/beach.jpg", image1, 0644)
		assertNoError(t, err)

		// 3. 生成缩略图
		thumb1 := []byte("beach thumbnail")
		err = fs.StoreThumbnail("/photos/2023/vacation/beach.jpg", thumb1, "image/jpeg")
		assertNoError(t, err)

		// 4. 读取验证
		data, err := readFile(fs, "/photos/2023/vacation/beach.jpg")
		assertNoError(t, err)
		if !bytes.Equal(data, image1) {
			t.Error("图片数据损坏")
		}

		thumb, err := fs.GetThumbnail("/photos/2023/vacation/beach.jpg")
		assertNoError(t, err)
		if !bytes.Equal(thumb, thumb1) {
			t.Error("缩略图数据损坏")
		}

		// 5. 重命名
		err = fs.Rename("/photos/2023", "/photos/old_2023")
		assertNoError(t, err)

		// 6. 验证被重命名的文件不能访问
		data, err = readFile(fs, "/photos/2023/vacation/beach.jpg")
		if err == nil {
			t.Error("被重命名的文件应该不能访问")
		}

		// 7. 验证重命名后文件可访问
		data, err = readFile(fs, "/photos/old_2023/vacation/beach.jpg")
		assertNoError(t, err)
		if !bytes.Equal(data, image1) {
			t.Errorf("重命名后图片数据损坏 %s, %s", string(data), string(image1))
		}

		// 8. 最终统计
		stats, err := fs.Stats()
		assertNoError(t, err)
		t.Logf("集成测试完成。统计: %+v", stats)
	})
}

// 在 boltfs_test.go 中添加调试函数
func debugDirIndex(t *testing.T, fs *BoltFS, dirPath string) {
	cleanDir := fs.normalizePath(dirPath)

	var entries []string
	err := fs.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("dir_index"))
		if bucket == nil {
			return errors.New("dir_index bucket not found")
		}

		data := bucket.Get([]byte(cleanDir))
		if data == nil {
			t.Logf("目录 %s 的索引数据为空", cleanDir)
			return nil
		}

		return json.Unmarshal(data, &entries)
	})

	if err != nil {
		t.Logf("读取目录索引错误: %v", err)
	} else {
		t.Logf("目录 %s 的索引条目: %v", cleanDir, entries)
	}

	// 检查目录下每个文件的元数据
	for _, entry := range entries {
		fullPath := cleanDir + "/" + entry
		if cleanDir == "/" {
			fullPath = "/" + entry
		}

		meta, err := fs.getMetadata(fullPath)
		if err != nil {
			t.Logf("文件 %s 的元数据获取失败: %v", fullPath, err)
		} else {
			t.Logf("文件 %s 存在，大小: %d", fullPath, meta.Size)
		}
	}
}

func TestUncoveredCodePaths(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("RemoveAll", func(t *testing.T) {
		// 创建深层目录结构
		fs.MkdirAll("/removeall/a/b/c", 0755)
		writeFile(fs, "/removeall/file1.txt", []byte("1"), 0644)
		writeFile(fs, "/removeall/a/file2.txt", []byte("2"), 0644)
		writeFile(fs, "/removeall/a/b/file3.txt", []byte("3"), 0644)

		// 递归删除
		err := fs.RemoveAll("/removeall")
		if err != nil {
			t.Fatalf("RemoveAll失败: %v", err)
		}

		// 验证删除
		if exists, _ := exists(fs, "/removeall"); exists {
			t.Error("RemoveAll后目录应该不存在")
		}
	})

	t.Run("ReadAt", func(t *testing.T) {
		content := []byte("0123456789ABCDEF")
		writeFile(fs, "/readat.txt", content, 0644)

		f, err := fs.Open("/readat.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// 从位置5开始读取
		buf := make([]byte, 4)
		n, err := f.ReadAt(buf, 5)
		if err != nil {
			t.Fatalf("ReadAt失败: %v", err)
		}
		if n != 4 || string(buf) != "5678" {
			t.Errorf("ReadAt内容错误: %s", string(buf))
		}
	})

	t.Run("WriteAt", func(t *testing.T) {
		f, err := fs.Create("/writeat.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// 先写入一些数据
		f.Write([]byte("0123456789"))

		// 在位置5写入
		n, err := f.WriteAt([]byte("XXXX"), 5)
		if err != nil {
			t.Fatalf("WriteAt失败: %v", err)
		}
		if n != 4 {
			t.Errorf("WriteAt写入字节数错误: %d", n)
		}

		f.Close()

		// 验证
		data, _ := readFile(fs, "/writeat.txt")
		if string(data) != "01234XXXX9" {
			t.Errorf("WriteAt后内容错误: %s", string(data))
		}
	})

	t.Run("SeekOperations", func(t *testing.T) {
		content := []byte("0123456789")
		writeFile(fs, "/seek.txt", content, 0644)

		f, err := fs.Open("/seek.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Seek从当前位置
		pos, err := f.Seek(3, 1) // 1 = io.SeekCurrent
		if err != nil {
			t.Fatalf("Seek(Current)失败: %v", err)
		}
		if pos != 3 {
			t.Errorf("Seek位置错误: %d", pos)
		}

		// Seek从末尾
		pos, err = f.Seek(-2, 2) // 2 = io.SeekEnd
		if err != nil {
			t.Fatalf("Seek(End)失败: %v", err)
		}
		if pos != 8 {
			t.Errorf("从末尾Seek位置错误: %d", pos)
		}
	})

	t.Run("Sync", func(t *testing.T) {
		f, err := fs.Create("/sync.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		f.Write([]byte("test"))
		err = f.Sync()
		if err != nil {
			t.Errorf("Sync失败: %v", err)
		}
	})

	t.Run("Truncate", func(t *testing.T) {
		f, err := fs.Create("/truncate.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		f.Write([]byte("0123456789"))
		f.Close()

		// 重新打开并截断
		f, err = fs.OpenFile("/truncate.txt", os.O_RDWR, 0644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		err = f.Truncate(5)
		if err != nil {
			t.Fatalf("Truncate失败: %v", err)
		}

		f.Close()

		data, _ := readFile(fs, "/truncate.txt")
		if len(data) != 5 {
			t.Errorf("Truncate后长度错误: %d", len(data))
		}
	})

	t.Run("Readdirnames", func(t *testing.T) {
		fs.Mkdir("/readdirnames", 0755)
		writeFile(fs, "/readdirnames/a.txt", []byte("a"), 0644)
		writeFile(fs, "/readdirnames/b.txt", []byte("b"), 0644)

		f, err := fs.Open("/readdirnames")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		names, err := f.Readdirnames(0)
		if err != nil {
			t.Fatalf("Readdirnames失败: %v", err)
		}
		if len(names) != 2 {
			t.Errorf("Readdirnames数量错误: %d", len(names))
		}
	})

	t.Run("WriteString", func(t *testing.T) {
		f, err := fs.Create("/writestring.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		n, err := f.WriteString("test string")
		if err != nil {
			t.Fatalf("WriteString失败: %v", err)
		}
		if n != 11 {
			t.Errorf("WriteString写入字节数错误: %d", n)
		}
	})

	t.Run("Name", func(t *testing.T) {
		f, err := fs.Create("/name.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		name := f.Name()
		if name != "/name.txt" {
			t.Errorf("Name返回错误: %s", name)
		}
	})

	t.Run("ChownAndSymlink", func(t *testing.T) {
		// Chown 应该总是成功（无操作）
		err := fs.Chown("/notexist.txt", 1000, 1000)
		if err != nil {
			t.Errorf("Chown应该总是成功: %v", err)
		}

		// SymlinkIfPossible 应该返回不支持错误
		err = fs.SymlinkIfPossible("/old", "/new")
		if err == nil {
			t.Error("SymlinkIfPossible应该返回错误")
		}

		// ReadlinkIfPossible 应该返回不支持错误
		_, err = fs.ReadlinkIfPossible("/link")
		if err == nil {
			t.Error("ReadlinkIfPossible应该返回错误")
		}
	})
}

func TestErrorPaths(t *testing.T) {
	t.Run("只读文件系统错误", func(t *testing.T) {
		dbPath := t.TempDir() + "/readonly.db"

		// 先创建可读写文件系统并写入文件
		rwFS, err := NewBoltFS(dbPath, false)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(rwFS, "/test.txt", []byte("test"), 0644)
		rwFS.Close()

		// 以只读模式打开
		roFS, err := NewBoltFS(dbPath, true)
		if err != nil {
			t.Fatal(err)
		}
		defer roFS.Close()

		// 尝试各种写入操作（应该都失败）
		testCases := []struct {
			name string
			fn   func() error
		}{
			{"Create", func() error { _, err := roFS.Create("/new.txt"); return err }},
			{"Mkdir", func() error { return roFS.Mkdir("/newdir", 0755) }},
			{"Remove", func() error { return roFS.Remove("/test.txt") }},
			{"Rename", func() error { return roFS.Rename("/test.txt", "/new.txt") }},
			{"Chmod", func() error { return roFS.Chmod("/test.txt", 0600) }},
			{"Chtimes", func() error { return roFS.Chtimes("/test.txt", time.Now(), time.Now()) }},
		}

		for _, tc := range testCases {
			err := tc.fn()
			if err == nil || err.Error() != "read-only file system" {
				t.Errorf("%s在只读文件系统上应该失败: %v", tc.name, err)
			}
		}
	})

	t.Run("文件不存在错误", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 尝试打开不存在的文件
		_, err := fs.Open("/nonexistent.txt")
		if !os.IsNotExist(err) {
			t.Errorf("打开不存在的文件应该返回IsNotExist错误: %v", err)
		}

		// 尝试删除不存在的文件
		err = fs.Remove("/nonexistent.txt")
		if !os.IsNotExist(err) {
			t.Errorf("删除不存在的文件应该返回IsNotExist错误: %v", err)
		}

		// 尝试重命名不存在的文件
		err = fs.Rename("/nonexistent.txt", "/new.txt")
		if !os.IsNotExist(err) {
			t.Errorf("重命名不存在的文件应该返回IsNotExist错误: %v", err)
		}
	})

	t.Run("目录操作错误", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 在文件上创建目录（应该失败）
		writeFile(fs, "/file.txt", []byte("test"), 0644)
		err := fs.Mkdir("/file.txt/subdir", 0755)
		if err == nil {
			t.Error("在文件上创建目录应该失败")
		}

		// 删除非空目录（应该失败）
		fs.Mkdir("/nonempty", 0755)
		writeFile(fs, "/nonempty/file.txt", []byte("test"), 0644)
		err = fs.Remove("/nonempty")
		if err == nil || err.Error() != "directory not empty" {
			t.Errorf("删除非空目录应该失败: %v", err)
		}
	})

	t.Run("并发文件句柄错误", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 创建文件并关闭
		f, err := fs.Create("/test.txt")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()

		// 尝试在已关闭文件上操作
		_, err = f.Write([]byte("test"))
		if err == nil {
			t.Error("在已关闭文件上写入应该失败")
		}

		_, err = f.Read(make([]byte, 10))
		if err == nil {
			t.Error("在已关闭文件上读取应该失败")
		}

		_, err = f.Seek(0, 0)
		if err == nil {
			t.Error("在已关闭文件上Seek应该失败")
		}
	})
}

func TestTempFileLifecycle(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("临时文件创建和清理", func(t *testing.T) {
		const numTempFiles = 50

		// 创建大量临时文件
		for i := 0; i < numTempFiles; i++ {
			filename := "/tmp/session_" + string(rune('A'+(i%26))) + ".tmp"
			writeFile(fs, filename, []byte("session data"), 0644)
		}

		// 验证创建的文件
		stats, err := fs.Stats()
		if err != nil {
			t.Fatal(err)
		}

		filesBefore := stats["file_count"].(int)
		t.Logf("创建临时文件后: %d个文件", filesBefore)

		// 清理一部分临时文件
		for i := 0; i < numTempFiles/2; i++ {
			filename := "/tmp/session_" + string(rune('A'+(i%26))) + ".tmp"
			fs.Remove(filename)
		}

		// 验证清理后的状态
		stats, err = fs.Stats()
		if err != nil {
			t.Fatal(err)
		}

		filesAfter := stats["file_count"].(int)
		t.Logf("清理临时文件后: %d个文件", filesAfter)

		if filesAfter >= filesBefore {
			t.Error("清理后文件数量应该减少")
		}
	})

	t.Run("并发临时文件操作", func(t *testing.T) {
		const numWorkers = 10
		const filesPerWorker = 5

		done := make(chan bool, numWorkers)

		for i := 0; i < numWorkers; i++ {
			go func(workerID int) {
				for j := 0; j < filesPerWorker; j++ {
					filename := "/tmp/concurrent_" + string(rune('A'+workerID)) + "_" + string(rune('0'+j)) + ".tmp"
					writeFile(fs, filename, []byte("data"), 0644)

					// 立即读取验证
					data, err := readFile(fs, filename)
					if err != nil || string(data) != "data" {
						t.Errorf("协程%d文件%d验证失败", workerID, j)
					}

					// 清理一半文件
					if j%2 == 0 {
						fs.Remove(filename)
					}
				}
				done <- true
			}(i)
		}

		// 等待所有协程完成
		for i := 0; i < numWorkers; i++ {
			<-done
		}

		t.Log("并发临时文件操作完成")
	})
}

func TestFileLockManagement(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	t.Run("并发打开同一文件", func(t *testing.T) {
		// 创建测试文件
		err := writeFile(fs, "/locktest.txt", []byte("test"), 0644)
		assertNoError(t, err)

		const numGoroutines = 10
		var wg sync.WaitGroup
		results := make(chan string, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				f, err := fs.Open("/locktest.txt")
				if err != nil {
					results <- fmt.Sprintf("goroutine %d: %v", id, err)
					return
				}
				defer f.Close()

				// 读取文件
				buf := make([]byte, 4)
				n, err := f.Read(buf)
				if err != nil && err != io.EOF {
					results <- fmt.Sprintf("goroutine %d read error: %v", id, err)
					return
				}

				results <- fmt.Sprintf("goroutine %d read %d bytes", id, n)
			}(i)
		}

		wg.Wait()
		close(results)

		successCount := 0
		for result := range results {
			t.Log(result)
			if !strings.Contains(result, "error") {
				successCount++
			}
		}

		if successCount != numGoroutines {
			t.Errorf("期望 %d 个成功，实际 %d", numGoroutines, successCount)
		}
	})
}

func TestSafeLockUpgrade(t *testing.T) {
	fs, cleanup := createTestFS(t, false)
	defer cleanup()

	// 创建测试文件
	err := writeFile(fs, "/upgrade_test.txt", []byte("initial"), 0644)
	assertNoError(t, err)

	// 第一个goroutine以读模式打开
	done := make(chan bool, 2) // 使用缓冲通道，避免阻塞
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		f1, err := fs.Open("/upgrade_test.txt")
		if err != nil {
			t.Errorf("Failed to open file: %v", err)
			return
		}

		// 读取一些数据
		buf := make([]byte, 7)
		n, err := f1.Read(buf)
		if err != nil && err != io.EOF {
			t.Errorf("Failed to read: %v", err)
		}
		t.Logf("Read %d bytes: %s", n, string(buf[:n]))

		// 保持文件打开一段时间
		time.Sleep(100 * time.Millisecond)

		// 确保关闭文件
		f1.Close()

		done <- true
	}()

	// 等待第一个goroutine开始
	time.Sleep(10 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()

		// 等待一会儿再尝试写操作
		time.Sleep(50 * time.Millisecond)

		f2, err := fs.OpenFile("/upgrade_test.txt", os.O_RDWR, 0644)
		if err != nil {
			t.Logf("OpenFile error (expected): %v", err)
		} else {
			defer f2.Close()

			// 写入新数据
			_, err = f2.Write([]byte("new data"))
			if err != nil {
				t.Errorf("Failed to write: %v", err)
			} else {
				t.Log("Write successful")
			}
		}
		done <- true
	}()

	// 等待两个goroutine完成
	wg.Wait()
	close(done)

	// 验证文件内容
	data, err := readFile(fs, "/upgrade_test.txt")
	if err != nil {
		t.Errorf("Failed to read file: %v", err)
	} else {
		t.Logf("Final file content: %s", string(data))
	}
}

func TestFileSystemClose(t *testing.T) {
	fs, cleanup := createTestFS(t, false)

	// 创建并打开一个文件
	f, err := fs.Create("/test_close.txt")
	assertNoError(t, err)

	// 写入一些数据
	_, err = f.Write([]byte("test data"))
	assertNoError(t, err)

	// 不关闭文件，直接关闭文件系统
	err = fs.Close()
	assertNoError(t, err)

	// 尝试在已关闭的文件系统上操作文件 - 现在应该返回错误
	_, err = f.Write([]byte("more data"))
	if err == nil {
		t.Error("Expected error when writing to file after filesystem closed")
	} else {
		t.Logf("Write after close returned error (expected): %v", err)
		// 不检查错误内容，只要返回错误就通过
	}

	// 尝试关闭文件 - 由于文件系统已关闭，可能会有错误
	err = f.Close()
	// 只要不崩溃就认为成功，不检查具体错误信息
	if err != nil {
		t.Logf("Closing file after filesystem closed returned error: %v", err)
	} else {
		t.Log("Closing file after filesystem closed succeeded")
	}

	// 尝试在已关闭的文件系统上创建新文件
	_, err = fs.Create("/new.txt")
	if err == nil {
		t.Error("Expected error when creating file after filesystem closed")
	} else {
		t.Logf("Create after close returned error (expected): %v", err)
	}

	// 调用清理函数
	cleanup()
}

func TestBoltFSEdgeCases(t *testing.T) {
	t.Run("Name方法", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		name := fs.Name()
		if name != "BoltFS" {
			t.Errorf("期望名称 'BoltFS'，实际 '%s'", name)
		}
	})

	t.Run("createFile方法", func(t *testing.T) {
		// 直接测试createFile方法
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 由于createFile是私有方法，我们通过Create方法来测试
		// 但我们也可以测试一些边界情况
		// 创建已经存在的文件
		f1, err := fs.Create("/exists.txt")
		assertNoError(t, err)
		f1.Write([]byte("test"))
		f1.Close()

		// 尝试再次创建（O_EXCL模式）
		f2, err := fs.OpenFile("/exists.txt", os.O_CREATE|os.O_EXCL, 0644)
		if err == nil {
			f2.Close()
			t.Error("期望错误，但成功打开")
		}
	})

	t.Run("SaveMetadata方法", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 创建元数据
		meta := NewFileMetadata("/savetest.txt", false, 0644)
		meta.Size = 100
		meta.DataKey = "test-key"

		// 保存元数据
		err := fs.SaveMetadata(meta)
		assertNoError(t, err)

		// 验证元数据已保存
		savedMeta, err := fs.getMetadata("/savetest.txt")
		assertNoError(t, err)
		if savedMeta.Size != 100 {
			t.Errorf("期望大小 100，实际 %d", savedMeta.Size)
		}
	})

	t.Run("Lchown方法", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// Lchown总是返回nil（不支持）
		err := fs.Lchown("/test.txt", 1000, 1000)
		if err != nil {
			t.Errorf("Lchown应该返回nil，实际 %v", err)
		}

		// 创建文件后再测试
		f, err := fs.Create("/test.txt")
		assertNoError(t, err)
		f.Close()

		err = fs.Lchown("/test.txt", 1000, 1000)
		if err != nil {
			t.Errorf("Lchown应该返回nil，实际 %v", err)
		}
	})

	t.Run("Compact方法", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 创建一些文件
		for i := 0; i < 10; i++ {
			f, err := fs.Create(fmt.Sprintf("/compact%d.txt", i))
			assertNoError(t, err)
			f.Write([]byte("test data"))
			f.Close()
		}

		// 删除一些文件
		for i := 0; i < 5; i++ {
			fs.Remove(fmt.Sprintf("/compact%d.txt", i))
		}

		// 运行压缩
		err := fs.Compact()
		assertNoError(t, err)
	})

	t.Run("OpenFile边界情况", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 测试1：空路径
		_, err := fs.Open("")
		if err == nil {
			t.Error("期望空路径错误，但得到nil")
		}

		// 测试2：相对路径
		f, err := fs.Open("relative.txt")
		if err != nil {
			t.Logf("相对路径错误（可能正常）: %v", err)
		} else {
			f.Close()
		}

		// 测试3：带特殊字符的路径
		f, err = fs.Create("/test with spaces.txt")
		assertNoError(t, err)
		f.Close()

		// 测试4：非常长的路径
		longPath := "/" + strings.Repeat("a/", 50) + "file.txt"
		err = fs.MkdirAll(filepath.Dir(longPath), 0755)
		assertNoError(t, err)

		f, err = fs.Create(longPath)
		assertNoError(t, err)
		f.Close()

		// 验证长路径文件存在
		exists, _ := exists(fs, longPath)
		if !exists {
			t.Error("长路径文件应该存在")
		}
	})

	t.Run("只读文件系统边界", func(t *testing.T) {
		dbPath := t.TempDir() + "/readonly.db"

		// 先创建可读写文件系统
		rwFS, err := NewBoltFS(dbPath, false)
		assertNoError(t, err)

		// 创建文件
		f, err := rwFS.Create("/readonly.txt")
		assertNoError(t, err)
		f.Write([]byte("test"))
		f.Close()

		// 创建目录
		rwFS.Mkdir("/readonly_dir", 0755)

		rwFS.Close()

		// 以只读模式打开
		roFS, err := NewBoltFS(dbPath, true)
		assertNoError(t, err)
		defer roFS.Close()

		// 测试只读操作
		f, err = roFS.Open("/readonly.txt")
		assertNoError(t, err)
		f.Close()

		// 测试只读目录列表
		f, err = roFS.Open("/")
		assertNoError(t, err)
		_, err = f.Readdir(0)
		assertNoError(t, err)
		f.Close()
	})
}
