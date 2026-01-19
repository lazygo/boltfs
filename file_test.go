package boltfs

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestBoltFileEdgeCases(t *testing.T) {

	t.Run("Stat方法", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 创建文件
		f, err := fs.Create("/stat_test.txt")
		assertNoError(t, err)

		// 写入数据
		_, err = f.Write([]byte("test data"))
		assertNoError(t, err)

		// 获取文件信息
		info, err := f.Stat()
		assertNoError(t, err)

		// 现在应该返回基本文件名 "stat_test.txt" 而不是完整路径
		if info.Name() != "stat_test.txt" {
			t.Errorf("期望名称 'stat_test.txt'，实际 '%s'", info.Name())
		}

		if info.Size() != 9 {
			t.Errorf("期望大小 9，实际 %d", info.Size())
		}

		if info.IsDir() {
			t.Error("应该是文件，不是目录")
		}

		f.Close()

		// 测试目录的Stat
		fs.Mkdir("/stat_dir", 0755)
		dirFile, err := fs.Open("/stat_dir")
		assertNoError(t, err)
		defer dirFile.Close()

		dirInfo, err := dirFile.Stat()
		assertNoError(t, err)

		if !dirInfo.IsDir() {
			t.Error("应该是目录，不是文件")
		}

		if dirInfo.Name() != "stat_dir" {
			t.Errorf("期望目录名称 'stat_dir'，实际 '%s'", dirInfo.Name())
		}

		// 测试根目录
		rootFile, err := fs.Open("/")
		assertNoError(t, err)
		defer rootFile.Close()

		rootInfo, err := rootFile.Stat()
		assertNoError(t, err)

		if !rootInfo.IsDir() {
			t.Error("根目录应该是目录")
		}

		if rootInfo.Name() != "/" {
			t.Errorf("期望根目录名称 '/'，实际 '%s'", rootInfo.Name())
		}
	})

	t.Run("文件操作错误路径", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 测试1：在目录上读写
		fs.Mkdir("/testdir", 0755)
		f, err := fs.Open("/testdir")
		assertNoError(t, err)

		// 在目录上读取
		buf := make([]byte, 10)
		_, err = f.Read(buf)
		if err == nil {
			t.Error("在目录上读取应该返回错误")
		}

		// 在目录上写入
		_, err = f.Write([]byte("test"))
		if err == nil {
			t.Error("在目录上写入应该返回错误")
		}

		f.Close()

		// 测试2：已关闭文件的操作
		f, err = fs.Create("/closed.txt")
		assertNoError(t, err)
		f.Close()

		_, err = f.Write([]byte("test"))
		if err == nil {
			t.Error("在已关闭文件上写入应该返回错误")
		}

		_, err = f.Read(buf)
		if err == nil {
			t.Error("在已关闭文件上读取应该返回错误")
		}

		_, err = f.Seek(0, 0)
		if err == nil {
			t.Error("在已关闭文件上Seek应该返回错误")
		}

		// 测试3：文件系统关闭后的操作
		fs2, _ := createTestFS(t, false)
		f2, err := fs2.Create("/test.txt")
		assertNoError(t, err)

		fs2.Close() // 关闭文件系统

		_, err = f2.Write([]byte("test"))
		if err == nil {
			t.Error("在已关闭文件系统的文件上写入应该返回错误")
		}

		// 关闭文件（应该处理文件系统已关闭的情况）
		err = f2.Close()
		if err != nil && !strings.Contains(err.Error(), "closed") {
			t.Logf("关闭文件返回错误（可能正常）: %v", err)
		}
	})

	t.Run("Truncate边界情况", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		// 创建文件
		f, err := fs.Create("/truncate.txt")
		assertNoError(t, err)

		// 写入数据
		_, err = f.Write([]byte("0123456789"))
		assertNoError(t, err)
		f.Close()

		// 重新打开并截断
		f, err = fs.OpenFile("/truncate.txt", os.O_RDWR, 0644)
		assertNoError(t, err)
		defer f.Close()

		// 截断为更小
		err = f.Truncate(5)
		assertNoError(t, err)

		// 验证大小
		info, _ := f.Stat()
		if info.Size() != 5 {
			t.Errorf("截断后期望大小 5，实际 %d", info.Size())
		}

		// 读取验证
		buf := make([]byte, 10)
		n, err := f.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			t.Errorf("读取错误: %v", err)
		}

		if string(buf[:n]) != "01234" {
			t.Errorf("期望内容 '01234'，实际 '%s'", string(buf[:n]))
		}

		// 测试负大小截断
		err = f.Truncate(-1)
		if err == nil {
			t.Error("负大小截断应该返回错误")
		}

		// 测试在目录上截断
		fs.Mkdir("/truncate_dir", 0755)
		dirFile, err := fs.Open("/truncate_dir")
		assertNoError(t, err)
		defer dirFile.Close()

		err = dirFile.Truncate(10)
		if err == nil {
			t.Error("在目录上截断应该返回错误")
		}
	})

	t.Run("ReadAt_偏移量边界", func(t *testing.T) {
		fs, cleanup := createTestFS(t, false)
		defer cleanup()

		f, err := fs.Create("/readat.txt")
		assertNoError(t, err)
		defer f.Close()

		_, err = f.Write([]byte("hello"))
		assertNoError(t, err)

		buf := make([]byte, 4)
		_, err = f.ReadAt(buf, -1)
		if err != ErrInvalidSeek {
			t.Errorf("负偏移应返回 ErrInvalidSeek，实际: %v", err)
		}

		n, err := f.ReadAt(buf, 10)
		if err != io.EOF || n != 0 {
			t.Errorf("越界读取期望 io.EOF 且 n=0，实际 n=%d err=%v", n, err)
		}
	})
}
