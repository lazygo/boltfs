# boltfs

# 1. 运行完整测试套件

go test ./... -timeout 30s

# 2. 生成最终覆盖率报告

go test ./... -coverprofile=final_coverage.out

go tool cover -html=final_coverage.out -o final_coverage.html

# 3. 检查是否有竞态条件

go test ./... -race -timeout 30s

# 4. 运行基准测试

go test ./... -bench=. -benchtime=3s -timeout 90s

```
goos: darwin
goarch: arm64
pkg: github.com/lazygo/boltfs
cpu: Apple M4 Pro
BenchmarkFileOperations/Write-1KB-14                 152          22761997 ns/op
BenchmarkFileOperations/Read-1KB-14              4349034               826.5 ns/op
BenchmarkFileOperations/Write-10KB-14                157          23145365 ns/op           0.44 MB/s
BenchmarkFileOperations/Stat-14                 18354676               183.3 ns/op
BenchmarkDirectoryOperations/MkdirAll-5-levels-14                     75          47860731 ns/op
BenchmarkDirectoryOperations/Readdir-100-files-14                3951260               951.2 ns/op
BenchmarkConcurrentOperations/Concurrent-Create-100-14               145          24198567 ns/op
BenchmarkConcurrentOperations/Concurrent-Read-Same-File-14               2196055              1620 ns/op
PASS
ok      github.com/lazygo/boltfs        54.600s

```

# 5. 查看最终覆盖率

go tool cover -func=final_coverage.out | tail -5