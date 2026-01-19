package boltfs

import (
	"testing"
)

func TestMetadataMethods(t *testing.T) {
	t.Run("RLock和RUnlock", func(t *testing.T) {
		meta := NewFileMetadata("/test.txt", false, 0644)

		// 测试RLock/RUnlock
		meta.RLock()
		defer meta.RUnlock()

		// 确保没有panic
		if meta.Name != "/test.txt" {
			t.Error("元数据名称错误")
		}
	})

	t.Run("GetSize和SetSize", func(t *testing.T) {
		meta := NewFileMetadata("/test.txt", false, 0644)

		// 测试设置和获取大小
		meta.SetSize(1024)
		if meta.GetSize() != 1024 {
			t.Errorf("期望大小 1024，实际 %d", meta.GetSize())
		}
	})

	t.Run("ToFileInfo_Sys方法", func(t *testing.T) {
		meta := NewFileMetadata("/test.txt", false, 0644)
		info := meta.ToFileInfo()

		// 测试Sys()方法
		if info.Sys() != nil {
			t.Error("Sys()应该返回nil")
		}
	})

	t.Run("Unmarshal错误处理", func(t *testing.T) {
		meta := NewFileMetadata("/test.txt", false, 0644)

		// 无效的JSON数据
		err := meta.Unmarshal([]byte("{invalid json"))
		if err == nil {
			t.Error("期望解析错误，但得到nil")
		}

		// 有效JSON但不匹配结构
		err = meta.Unmarshal([]byte(`{"name":123}`))
		if err == nil {
			t.Error("期望类型不匹配错误，但得到nil")
		}
	})

	t.Run("ToFileInfo路径边界", func(t *testing.T) {
		meta := NewFileMetadata("/dir/subdir/", true, 0755)
		info := meta.ToFileInfo()
		if info.Name() != "subdir" {
			t.Errorf("期望名称 'subdir'，实际 '%s'", info.Name())
		}

		rootMeta := NewFileMetadata("/", true, 0755)
		rootInfo := rootMeta.ToFileInfo()
		if rootInfo.Name() != "/" {
			t.Errorf("期望根目录名称 '/'，实际 '%s'", rootInfo.Name())
		}
	})

	t.Run("Marshal_和_Clone", func(t *testing.T) {
		meta := NewFileMetadata("/clone.json", false, 0644)
		meta.SetSize(2048)
		meta.ContentType = "application/json"
		meta.ThumbnailKey = "thumb-1"

		data, err := meta.Marshal()
		if err != nil {
			t.Fatalf("Marshal失败: %v", err)
		}

		var decoded FileMetadata
		if err := decoded.Unmarshal(data); err != nil {
			t.Fatalf("Unmarshal失败: %v", err)
		}

		if decoded.Name != meta.Name || decoded.Size != meta.Size || decoded.ContentType != meta.ContentType {
			t.Error("序列化/反序列化后字段不一致")
		}

		clone := meta.Clone()
		clone.Name = "/changed.json"
		if meta.Name == clone.Name {
			t.Error("Clone应返回独立对象")
		}
	})
}
