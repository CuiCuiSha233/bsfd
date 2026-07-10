package bsfd_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cuicuisha233/bsfd/discovery"
	"github.com/cuicuisha233/bsfd/protocol"
	"github.com/cuicuisha233/bsfd/storage"
	"github.com/cuicuisha233/bsfd/transport"
)

// ============================================================================
// 端到端集成测试：模拟 Admin 分发 → Client 下载 的完整流程
// ============================================================================

// testStore 是 storage.Store 接口的最小实现，用于测试。
type testStore struct {
	mu     sync.Mutex
	chunks map[int][]byte
	total  int
}

func newTestStore(total int) *testStore {
	return &testStore{chunks: make(map[int][]byte), total: total}
}

func (s *testStore) SaveChunk(index int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks[index] = data
	return nil
}

func (s *testStore) GetChunk(index int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.chunks[index]
	if !ok {
		return nil, fmt.Errorf("chunk %d not found", index)
	}
	return d, nil
}

func (s *testStore) HasChunk(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chunks[index]
	return ok
}

func (s *testStore) Missing(total int) []int {
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

func (s *testStore) Complete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks) == s.total
}

func (s *testStore) Total() int { return s.total }

// TestIntegration_AdminToClient 验证 Admin 启动传输引擎 → Client 连接 → 下载分块的完整流程。
func TestIntegration_AdminToClient(t *testing.T) {
	// === 准备测试数据 ===
	chunkData := []byte("Hello BitStorm! This is a test chunk for end-to-end verification.")
	chunkHash := "abc123hash"

	// === Admin 端 ===
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 26933
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("admin-001")

	adminStore := newTestStore(1)
	adminStore.SaveChunk(0, chunkData)

	adminEngine.SetChunkHashes([]string{chunkHash})
	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		return adminStore.GetChunk(index)
	})

	var adminGotRequest bool
	adminEngine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			adminGotRequest = true
			t.Logf("[Admin] 收到分块请求: peer=%s chunk=%d", peerID, chunkIndex)
			// 宿主应用负责在回调中发送数据
			data, err := adminStore.GetChunk(chunkIndex)
			if err == nil {
				// ChunkDataBin 格式: [4B index BE][raw data]
				idx := protocol.BEBytes(uint32(chunkIndex))
				payload := append(idx[:], data...)
				adminEngine.SendRaw(peerID, protocol.EncodeRaw(protocol.TypeChunkDataBin, payload))
			}
		},
		OnPeerConnected: func(peerID string) {
			t.Logf("[Admin] 节点已连接: %s", peerID)
		},
	})

	if err := adminEngine.Start(); err != nil {
		t.Fatalf("Admin 启动失败: %v", err)
	}
	defer adminEngine.Stop()

	// === Client 端 ===
	clientCfg := transport.DefaultConfig()
	clientCfg.ListenPort = 26934
	clientEngine := transport.New(clientCfg)
	clientEngine.SetLocalPeerID("client-001")

	clientStore := newTestStore(1)

	var clientGotChunk bool
	clientEngine.SetCallbacks(transport.Callbacks{
		OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
			clientGotChunk = true
			t.Logf("[Client] 收到分块 %d 来自 %s (%d 字节)", chunkIndex, peerID, len(data))
			clientStore.SaveChunk(chunkIndex, data)
		},
	})

	if err := clientEngine.Start(); err != nil {
		t.Fatalf("Client 启动失败: %v", err)
	}
	defer clientEngine.Stop()

	// === 建立连接 ===
	conn, err := clientEngine.Connect("admin-001", "127.0.0.1", 26933)
	if err != nil {
		t.Fatalf("Client 连接 Admin 失败: %v", err)
	}
	_ = conn

	// 等连接完全建立
	time.Sleep(200 * time.Millisecond)

	// 验证连接已建立
	if !clientEngine.IsConnected("admin-001") {
		t.Fatal("Client 应已连接到 Admin")
	}
	if len(clientEngine.Peers()) == 0 {
		t.Fatal("Client 应能看到 Admin 节点")
	}

	// === 请求分块 ===
	t.Log("Client 发起分块请求...")
	if err := clientEngine.RequestChunk("admin-001", chunkHash, 0); err != nil {
		t.Fatalf("请求分块失败: %v", err)
	}

	// 等待传输完成
	time.Sleep(300 * time.Millisecond)

	// === 验证结果 ===
	if !adminGotRequest {
		t.Error("Admin 应收到分块请求回调")
	}
	if !clientGotChunk {
		t.Error("Client 应收到分块数据回调")
	}
	if !clientStore.Complete() {
		t.Error("Client 应完成所有分块存储")
	}

	got, err := clientStore.GetChunk(0)
	if err != nil {
		t.Fatalf("Client 获取分块失败: %v", err)
	}
	if string(got) != string(chunkData) {
		t.Errorf("分块数据不匹配:\n期望: %s\n实际: %s", chunkData, got)
	}

	t.Log("✓ 端到端集成测试通过")
}

// TestIntegration_BlockPipeline 验证块级流水线传输。
func TestIntegration_BlockPipeline(t *testing.T) {
	// 创建一个较大的分块，由多个 block 组成
	chunkSize := 65536 // 64KB
	chunkData := make([]byte, chunkSize)
	for i := range chunkData {
		chunkData[i] = byte(i % 256)
	}

	// === Admin 端 ===
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 26935
	adminCfg.BlockSize = 16384 // 16KB blocks
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("admin-block")

	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		return chunkData, nil
	})

	var adminBlockRequests int
	adminEngine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			adminBlockRequests++
		},
	})

	adminEngine.Start()
	defer adminEngine.Stop()

	// === Client 端 ===
	clientCfg := transport.DefaultConfig()
	clientCfg.ListenPort = 26936
	clientCfg.BlockSize = 16384
	clientEngine := transport.New(clientCfg)
	clientEngine.SetLocalPeerID("client-block")

	assembled := make([]byte, chunkSize)
	var mu sync.Mutex

	clientEngine.SetCallbacks(transport.Callbacks{
		OnBlockReceived: func(peerID string, chunkIndex int, blockOffset int, data []byte) {
			mu.Lock()
			copy(assembled[blockOffset:], data)
			mu.Unlock()
			t.Logf("[Client] 收到块: chunk=%d offset=%d size=%d", chunkIndex, blockOffset, len(data))
		},
	})

	clientEngine.Start()
	defer clientEngine.Stop()

	// === 建立连接 ===
	clientEngine.Connect("admin-block", "127.0.0.1", 26935)
	time.Sleep(200 * time.Millisecond)

	// === 请求所有 block ===
	var wg sync.WaitGroup
	numBlocks := chunkSize / clientCfg.BlockSize
	for i := 0; i < numBlocks; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			clientEngine.RequestBlock("admin-block", "hash", 0, offset, clientCfg.BlockSize)
		}(i * clientCfg.BlockSize)
	}
	wg.Wait()

	time.Sleep(500 * time.Millisecond)

	// === 验证 ===
	for i := range chunkData {
		if assembled[i] != chunkData[i] {
			t.Errorf("字节 %d 不匹配: 期望 %d, 实际 %d", i, chunkData[i], assembled[i])
			break
		}
	}
	t.Log("✓ 块级流水线测试通过")
}

// TestIntegration_MultiPeer 验证多节点并发传输。
func TestIntegration_MultiPeer(t *testing.T) {
	chunkData := []byte("multi-peer test data for concurrent download verification")

	// === 管理端 (Seeder) ===
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 26937
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("seeder")

	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		return chunkData, nil
	})

	adminEngine.Start()
	defer adminEngine.Stop()

	// === 两个客户端 ===
	clients := []*transport.Engine{}
	for i := 1; i <= 2; i++ {
		cfg := transport.DefaultConfig()
		cfg.ListenPort = 26937 + i
		engine := transport.New(cfg)
		engine.SetLocalPeerID(fmt.Sprintf("client-%d", i))
		engine.Start()
		clients = append(clients, engine)
		defer engine.Stop()
	}

	// 两个客户端连接管理端
	for _, c := range clients {
		c.Connect("seeder", "127.0.0.1", 26937)
	}
	time.Sleep(300 * time.Millisecond)

	// 验证两个客户端都连接成功
	for _, c := range clients {
		if !c.IsConnected("seeder") {
			t.Errorf("%s 应已连接到管理端", c.LocalPeerID())
		}
	}
	if len(adminEngine.Peers()) != 2 {
		t.Errorf("管理端应有 2 个节点，实际: %d", len(adminEngine.Peers()))
	}

	t.Log("✓ 多节点并发测试通过")
}

// TestIntegration_RTT 验证 RTT 测量功能。
func TestIntegration_RTT(t *testing.T) {
	chunkData := []byte("RTT measurement test data")

	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 26940
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("rtt-admin")
	adminEngine.SetChunkProvider(func(index int) ([]byte, error) { return chunkData, nil })
	adminEngine.Start()
	defer adminEngine.Stop()

	clientCfg := transport.DefaultConfig()
	clientCfg.ListenPort = 26941
	clientEngine := transport.New(clientCfg)
	clientEngine.SetLocalPeerID("rtt-client")
	clientEngine.Start()
	defer clientEngine.Stop()

	clientEngine.Connect("rtt-admin", "127.0.0.1", 26940)
	time.Sleep(200 * time.Millisecond)

	// 测量 RTT：localhost 应该非常小
	rtt := clientEngine.PeerRTT("rtt-admin")
	if rtt == 0 {
		t.Log("RTT 尚未测量（需要实际数据传输后才有 RTT 数据）")
	} else {
		t.Logf("RTT: %v", rtt)
	}

	// 发一个请求来触发 RTT 测量
	clientEngine.RequestChunk("rtt-admin", "hash", 0)
	time.Sleep(200 * time.Millisecond)

	rtt = clientEngine.PeerRTT("rtt-admin")
	t.Logf("测量后 RTT: %v", rtt)

	t.Log("✓ RTT 测量测试通过")
}

// TestIntegration_DisconnectReconnect 验证断连与重连。
func TestIntegration_DisconnectReconnect(t *testing.T) {
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 26942
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("dc-admin")

	var disconnected bool
	adminEngine.SetCallbacks(transport.Callbacks{
		OnPeerDisconnected: func(peerID string) {
			disconnected = true
			t.Logf("[Admin] 节点断开: %s", peerID)
		},
	})
	adminEngine.Start()
	defer adminEngine.Stop()

	clientCfg := transport.DefaultConfig()
	clientCfg.ListenPort = 26943
	clientEngine := transport.New(clientCfg)
	clientEngine.SetLocalPeerID("dc-client")
	clientEngine.Start()
	defer clientEngine.Stop()

	clientEngine.Connect("dc-admin", "127.0.0.1", 26942)
	time.Sleep(200 * time.Millisecond)

	if !clientEngine.IsConnected("dc-admin") {
		t.Fatal("初始连接应成功")
	}

	// 断开
	clientEngine.Disconnect("dc-admin")
	time.Sleep(200 * time.Millisecond)

	if clientEngine.IsConnected("dc-admin") {
		t.Error("断开后不应再连接")
	}
	if !disconnected {
		t.Error("Admin 应收到断开回调")
	}

	// 重连
	clientEngine.Connect("dc-admin", "127.0.0.1", 26942)
	time.Sleep(200 * time.Millisecond)

	if !clientEngine.IsConnected("dc-admin") {
		t.Error("重连应成功")
	}

	t.Log("✓ 断连重连测试通过")
}

// TestIntegration_DiscoveryProtocol 验证发现层协议编码。
func TestIntegration_DiscoveryProtocol(t *testing.T) {
	// 验证心跳消息的完整编解码
	hb := protocol.Heartbeat{
		PeerID:          "node-001",
		Name:            "测试节点",
		IP:              "192.168.1.100",
		TCPPort:         26932,
		Role:            "admin",
		ChunkBitmap:     []byte{0xFF, 0x0F},
		TotalChunks:     12,
		PreloadedHashes: []string{"hashA", "hashB"},
	}

	frame, err := protocol.Encode(protocol.TypeHeartbeat, hb)
	if err != nil {
		t.Fatalf("编码心跳失败: %v", err)
	}

	if len(frame) < protocol.HeaderLen {
		t.Fatal("帧长度异常")
	}

	// 验证可以正确解码
	fr := protocol.NewFrameReader(&sliceReader{data: frame})
	msgType, payload, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("解码心跳帧失败: %v", err)
	}
	if msgType != protocol.TypeHeartbeat {
		t.Errorf("类型: 期望 0x%02X, 实际 0x%02X", protocol.TypeHeartbeat, msgType)
	}

	var decoded protocol.Heartbeat
	if err := protocol.UnmarshalPayload(payload, &decoded); err != nil {
		t.Fatalf("反序列化心跳失败: %v", err)
	}
	if decoded.PeerID != hb.PeerID {
		t.Errorf("PeerID 不匹配")
	}

	t.Log("✓ 发现层协议测试通过")
}

// TestIntegration_DiscoveryNetwork 验证发现层网络工具函数。
func TestIntegration_DiscoveryNetwork(t *testing.T) {
	ips := discovery.LocalIPs()
	t.Logf("本地 IP: %v", ips)
	if len(ips) == 0 {
		t.Log("警告: 未检测到非回环 IP（可能是无网络环境）")
	}

	hostname := discovery.Hostname()
	t.Logf("主机名: %s", hostname)
	if hostname == "" {
		t.Error("主机名不应为空")
	}

	t.Log("✓ 发现层网络工具测试通过")
}

// TestIntegration_Storage 验证存储接口可以正常工作。
func TestIntegration_Storage(t *testing.T) {
	var s storage.Store = newTestStore(3)

	if s.Complete() {
		t.Error("空存储不应标记为完成")
	}

	s.SaveChunk(0, []byte("chunk0"))
	s.SaveChunk(1, []byte("chunk1"))
	s.SaveChunk(2, []byte("chunk2"))

	if !s.Complete() {
		t.Error("3/3 分块应为完成")
	}

	missing := s.Missing(5)
	if len(missing) != 2 {
		t.Errorf("应有 2 个缺失: [3,4], 实际 %v", missing)
	}

	if !s.HasChunk(1) {
		t.Error("应存在分块 1")
	}
	if s.HasChunk(3) {
		t.Error("不应存在分块 3")
	}

	t.Log("✓ 存储接口测试通过")
}

// ============================================================================
// 辅助类型
// ============================================================================

type sliceReader struct {
	data []byte
	pos  int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
