# boltfs

# 1. 运行完整测试套件
go test ./vfs -timeout 30s

# 2. 生成最终覆盖率报告
go test ./vfs -coverprofile=final_coverage.out
go tool cover -html=final_coverage.out -o final_coverage.html

# 3. 检查是否有竞态条件
go test ./vfs -race -timeout 30s

# 4. 运行基准测试
go test ./vfs -bench=. -benchtime=3s -timeout 60s

# 5. 查看最终覆盖率
go tool cover -func=final_coverage.out | tail -5