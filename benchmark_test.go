package boltfs

import (
	"crypto/rand"
	"testing"
)

func BenchmarkFileOperations(b *testing.B) {
	dbPath := b.TempDir() + "/bench.db"
	fs, err := NewBoltFS(dbPath, false)
	if err != nil {
		b.Fatal(err)
	}
	defer fs.Close()

	// 准备测试数据
	data1KB := make([]byte, 1024)
	rand.Read(data1KB)

	data10KB := make([]byte, 10240)
	rand.Read(data10KB)

	b.ResetTimer()

	b.Run("Write-1KB", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := b.TempDir() + "/write_1kb.bin"
			f, err := fs.Create(path)
			if err != nil {
				b.Fatal(err)
			}
			f.Write(data1KB)
			f.Close()
		}
	})

	b.Run("Read-1KB", func(b *testing.B) {
		// 先准备数据
		path := b.TempDir() + "/read_1kb.bin"
		f, err := fs.Create(path)
		if err != nil {
			b.Fatal(err)
		}
		f.Write(data1KB)
		f.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, err := fs.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			buf := make([]byte, 1024)
			f.Read(buf)
			f.Close()
		}
	})

	b.Run("Write-10KB", func(b *testing.B) {
		b.SetBytes(10240)
		for i := 0; i < b.N; i++ {
			path := b.TempDir() + "/write_10kb.bin"
			f, err := fs.Create(path)
			if err != nil {
				b.Fatal(err)
			}
			f.Write(data10KB)
			f.Close()
		}
	})

	b.Run("Stat", func(b *testing.B) {
		path := b.TempDir() + "/stat_test.txt"
		f, err := fs.Create(path)
		if err != nil {
			b.Fatal(err)
		}
		f.Write(data1KB)
		f.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			fs.Stat(path)
		}
	})
}

func BenchmarkDirectoryOperations(b *testing.B) {
	dbPath := b.TempDir() + "/dir_bench.db"
	fs, err := NewBoltFS(dbPath, false)
	if err != nil {
		b.Fatal(err)
	}
	defer fs.Close()

	b.Run("MkdirAll-5-levels", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := b.TempDir() + "/a/b/c/d/e"
			fs.MkdirAll(path, 0755)
		}
	})

	b.Run("Readdir-100-files", func(b *testing.B) {
		// 准备目录和文件
		dirPath := b.TempDir() + "/many_files"
		fs.MkdirAll(dirPath, 0755)

		for i := 0; i < 100; i++ {
			path := dirPath + "/file_" + string(rune('A'+(i%26))) + ".txt"
			f, _ := fs.Create(path)
			f.Write([]byte("test"))
			f.Close()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, err := fs.Open(dirPath)
			if err != nil {
				b.Fatal(err)
			}
			f.Readdir(0)
			f.Close()
		}
	})
}

func BenchmarkConcurrentOperations(b *testing.B) {
	dbPath := b.TempDir() + "/concurrent_bench.db"
	fs, err := NewBoltFS(dbPath, false)
	if err != nil {
		b.Fatal(err)
	}
	defer fs.Close()

	b.Run("Concurrent-Create-100", func(b *testing.B) {
		b.SetParallelism(10)
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				path := b.TempDir() + "/concurrent_file_" + string(rune('A'+(counter%26))) + ".txt"
				f, err := fs.Create(path)
				if err != nil {
					b.Fatal(err)
				}
				f.Write([]byte("concurrent data"))
				f.Close()
				counter++
			}
		})
	})

	b.Run("Concurrent-Read-Same-File", func(b *testing.B) {
		// 准备一个共享文件
		sharedPath := b.TempDir() + "/shared.txt"
		f, err := fs.Create(sharedPath)
		if err != nil {
			b.Fatal(err)
		}
		f.Write([]byte("shared data for concurrent reading"))
		f.Close()

		b.SetParallelism(20)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				f, err := fs.Open(sharedPath)
				if err != nil {
					b.Fatal(err)
				}
				buf := make([]byte, 100)
				f.Read(buf)
				f.Close()
			}
		})
	})
}
