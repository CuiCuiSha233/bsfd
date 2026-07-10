package storage_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cuicuisha233/bsfd/storage"
)

// ============================================================================
// 参考实现：内存存储
// ============================================================================

type memStore struct {
	mu     sync.Mutex
	chunks map[int][]byte
	total  int
}

func newMemStore(total int) *memStore {
	return &memStore{chunks: make(map[int][]byte), total: total}
}

func (s *memStore) SaveChunk(index int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.chunks[index]; ok {
		if string(existing) != string(data) {
			return fmt.Errorf("chunk %d: data mismatch", index)
		}
		return nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	s.chunks[index] = cp
	return nil
}

func (s *memStore) GetChunk(index int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.chunks[index]
	if !ok {
		return nil, fmt.Errorf("chunk %d not found", index)
	}
	return d, nil
}

func (s *memStore) HasChunk(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chunks[index]
	return ok
}

func (s *memStore) Missing(total int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var missing []int
	for i := 0; i < total; i++ {
		if _, ok := s.chunks[i]; !ok {
			missing = append(missing, i)
		}
	}
	return missing
}

func (s *memStore) Complete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks) == s.total
}

func (s *memStore) Total() int { return s.total }

var _ storage.Store = (*memStore)(nil)

// ============================================================================
// Store 接口基础测试
// ============================================================================

func TestStore_Empty(t *testing.T) {
	s := newMemStore(3)
	if s.Complete() {
		t.Error("空存储不应 Complete")
	}
	if len(s.Missing(3)) != 3 {
		t.Error("3/0 空存储应缺失 3 个")
	}
}

func TestStore_SaveGet(t *testing.T) {
	s := newMemStore(3)
	data := []byte("hello chunk")

	if err := s.SaveChunk(0, data); err != nil {
		t.Fatalf("SaveChunk 失败: %v", err)
	}

	if !s.HasChunk(0) {
		t.Error("HasChunk(0) 应为 true")
	}
	if s.HasChunk(1) {
		t.Error("HasChunk(1) 应为 false")
	}

	got, err := s.GetChunk(0)
	if err != nil {
		t.Fatalf("GetChunk 失败: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("数据不匹配: 期望 %q, 实际 %q", data, got)
	}
}

func TestStore_GetMissing(t *testing.T) {
	s := newMemStore(3)

	_, err := s.GetChunk(0)
	if err == nil {
		t.Error("缺失分块 GetChunk 应报错")
	}
}

func TestStore_SaveDuplicate_SameData(t *testing.T) {
	s := newMemStore(3)
	data := []byte("same")
	s.SaveChunk(0, data)
	if err := s.SaveChunk(0, data); err != nil {
		t.Errorf("相同数据重复保存不应报错: %v", err)
	}
}

func TestStore_SaveDuplicate_DifferentData(t *testing.T) {
	s := newMemStore(3)
	s.SaveChunk(0, []byte("data1"))
	if err := s.SaveChunk(0, []byte("data2")); err == nil {
		t.Error("不同数据重复保存应报错")
	}
}

func TestStore_Missing(t *testing.T) {
	s := newMemStore(5)
	s.SaveChunk(0, []byte("c0"))
	s.SaveChunk(2, []byte("c2"))
	s.SaveChunk(4, []byte("c4"))

	missing := s.Missing(5)
	if len(missing) != 2 {
		t.Errorf("应有 2 个缺失, 实际 %v", missing)
	}
	for _, idx := range missing {
		if idx != 1 && idx != 3 {
			t.Errorf("意外缺失: %d", idx)
		}
	}
}

func TestStore_Complete(t *testing.T) {
	s := newMemStore(3)
	if s.Complete() {
		t.Error("0/3 不应 Complete")
	}

	s.SaveChunk(0, []byte("c0"))
	s.SaveChunk(1, []byte("c1"))
	if s.Complete() {
		t.Error("2/3 不应 Complete")
	}

	s.SaveChunk(2, []byte("c2"))
	if !s.Complete() {
		t.Error("3/3 应 Complete")
	}
}

func TestStore_Total(t *testing.T) {
	s := newMemStore(100)
	if s.Total() != 100 {
		t.Errorf("Total: 期望 100, 实际 %d", s.Total())
	}
}

func TestStore_SaveChunk_DataCopy(t *testing.T) {
	// 验证 Store 拷贝了数据（而非保存原始引用）
	s := newMemStore(1)
	original := []byte("original")
	s.SaveChunk(0, original)
	original[0] = 'X' // 修改原始

	got, _ := s.GetChunk(0)
	if string(got) != "original" {
		t.Errorf("数据应被拷贝: 期望 original, 实际 %s", got)
	}
}

// ============================================================================
// 并发安全测试
// ============================================================================

func TestStore_ConcurrentSave(t *testing.T) {
	s := newMemStore(100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s.SaveChunk(idx, []byte{byte(idx)})
		}(i)
	}
	wg.Wait()

	if !s.Complete() {
		t.Error("并发保存后应 Complete")
	}
}

func TestStore_ConcurrentSaveGet(t *testing.T) {
	s := newMemStore(50)
	// 先保存
	for i := 0; i < 50; i++ {
		s.SaveChunk(i, []byte{byte(i)})
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			data, err := s.GetChunk(idx)
			if err != nil || data[0] != byte(idx) {
				t.Errorf("Chunk %d: err=%v data=%v", idx, err, data)
			}
		}(i)
	}
	wg.Wait()
}

func TestStore_ConcurrentSaveGetMixed(t *testing.T) {
	s := newMemStore(50)
	var wg sync.WaitGroup

	// 一半 goroutine 写，一半读
	for i := 0; i < 25; i++ {
		idx := i
		s.SaveChunk(idx, []byte{byte(idx)}) // 先存好
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				s.SaveChunk(idx, []byte{byte(idx)})
			} else {
				s.GetChunk(idx) // 可能不存在
			}
		}(i)
	}
	wg.Wait()
	// 不应 panic
}

// ============================================================================
// 边界测试
// ============================================================================

func TestStore_SingleChunk(t *testing.T) {
	s := newMemStore(1)
	if s.Complete() {
		t.Error("初始不应 Complete")
	}
	s.SaveChunk(0, []byte("x"))
	if !s.Complete() {
		t.Error("单分块应 Complete")
	}
}

func TestStore_ZeroTotal(t *testing.T) {
	s := newMemStore(0)
	if !s.Complete() {
		t.Error("0 分块的 Store 应 Complete")
	}
	missing := s.Missing(0)
	if len(missing) != 0 {
		t.Errorf("0 分块 Missing 应为空, 实际 %v", missing)
	}
}

func TestStore_LargeChunkData(t *testing.T) {
	s := newMemStore(2)
	large := make([]byte, 1<<20) // 1MB
	for i := range large {
		large[i] = byte(i % 256)
	}
	s.SaveChunk(0, large)

	got, _ := s.GetChunk(0)
	if len(got) != len(large) {
		t.Errorf("长度: 期望 %d, 实际 %d", len(large), len(got))
	}
	// 抽样验证
	for _, idx := range []int{0, len(large) / 2, len(large) - 1} {
		if got[idx] != large[idx] {
			t.Errorf("索引 %d: 期望 %d, 实际 %d", idx, large[idx], got[idx])
		}
	}
}

func TestStore_NegativeIndex(t *testing.T) {
	s := newMemStore(1)
	// GetChunk 下标为负时的行为（Go map 不会 panic, 但返回的 ok=false）
	if s.HasChunk(-1) {
		t.Error("负数下标不应存在")
	}
	_, err := s.GetChunk(-1)
	if err == nil {
		t.Error("负数下标 GetChunk 应返回错误")
	}
}

// ============================================================================
// Missing 方法边界测试
// ============================================================================

func TestStore_Missing_ZeroTotal(t *testing.T) {
	s := newMemStore(0)
	missing := s.Missing(0)
	if len(missing) != 0 {
		t.Errorf("total=0 Missing 应为空: %v", missing)
	}
}

func TestStore_Missing_LargerThanTotal(t *testing.T) {
	s := newMemStore(3)
	// Missing(5) 检查 [0,5) 范围（超出 Total 的分块也算缺失）
	s.SaveChunk(0, []byte("c0"))
	s.SaveChunk(1, []byte("c1"))
	s.SaveChunk(2, []byte("c2"))

	missing := s.Missing(5)
	if len(missing) != 2 {
		t.Errorf("Missing(5) 应有 2 个（3 和 4）, 实际 %v", missing)
	}
	for _, idx := range missing {
		if idx != 3 && idx != 4 {
			t.Errorf("意外缺失: %d", idx)
		}
	}
}

// ============================================================================
// 大数据量压力测试
// ============================================================================

func TestStore_ManyChunks(t *testing.T) {
	const numChunks = 1000
	s := newMemStore(numChunks)

	for i := 0; i < numChunks; i++ {
		s.SaveChunk(i, []byte{byte(i % 256), byte((i >> 8) % 256)})
	}

	if !s.Complete() {
		t.Errorf("%d 分块应 Complete", numChunks)
	}

	// 验证随机抽样的分块
	for _, idx := range []int{0, numChunks / 2, numChunks - 1} {
		if !s.HasChunk(idx) {
			t.Errorf("分块 %d 应存在", idx)
		}
		data, err := s.GetChunk(idx)
		if err != nil {
			t.Errorf("获取分块 %d 失败: %v", idx, err)
		}
		if len(data) != 2 {
			t.Errorf("分块 %d 长度: 期望 2, 实际 %d", idx, len(data))
		}
	}
}
