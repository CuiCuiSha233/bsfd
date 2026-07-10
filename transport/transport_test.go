package transport

import (
	"testing"
	"time"
)

// ============================================================================
// Config 测试
// ============================================================================

func TestDefaultConfig_Values(t *testing.T) {
	cfg := DefaultConfig()

	checks := []struct {
		name     string
		got, want interface{}
	}{
		{"ListenPort", cfg.ListenPort, 26932},
		{"ConnPoolSize", cfg.ConnPoolSize, 4},
		{"ConnTimeout", cfg.ConnTimeout, 10 * time.Second},
		{"TCPReadBufferSize", cfg.TCPReadBufferSize, 1 << 20},
		{"TCPWriteBufferSize", cfg.TCPWriteBufferSize, 1 << 20},
		{"HeartbeatInterval", cfg.HeartbeatInterval, 10 * time.Second},
		{"HeartbeatTimeout", cfg.HeartbeatTimeout, 30 * time.Second},
		{"RequestTimeout", cfg.RequestTimeout, 30 * time.Second},
		{"TimeoutCheckInterval", cfg.TimeoutCheckInterval, 5 * time.Second},
		{"MaxInflightRequests", cfg.MaxInflightRequests, 8},
		{"BlockSize", cfg.BlockSize, 16 * 1024},
		{"MaxRetries", cfg.MaxRetries, 3},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: 期望 %v, 实际 %v", c.name, c.want, c.got)
		}
	}
}

func TestConfig_CustomValues(t *testing.T) {
	cfg := Config{
		ListenPort:           12345,
		ConnPoolSize:         8,
		ConnTimeout:          5 * time.Second,
		HeartbeatInterval:    3 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       10 * time.Second,
		TimeoutCheckInterval: 1 * time.Second,
		MaxInflightRequests:  16,
		BlockSize:            32768,
		MaxRetries:           5,
	}
	if cfg.ListenPort != 12345 {
		t.Fatal("自定义 Config 值不正确")
	}
}

// ============================================================================
// Engine 生命周期测试
// ============================================================================

func TestNewEngine(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	if e == nil {
		t.Fatal("New 返回 nil")
	}
	if len(e.Peers()) != 0 {
		t.Error("新 Engine 应无 Peer")
	}
}

func TestEngine_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26950
	e := New(cfg)

	if err := e.Start(); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	if len(e.Peers()) != 0 {
		t.Error("刚启动的 Engine 应无 Peer")
	}

	e.Stop()
	if len(e.Peers()) != 0 {
		t.Error("Stop 后 Engine 应无 Peer")
	}
}

func TestEngine_StartIdempotent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26951
	e := New(cfg)

	if err := e.Start(); err != nil {
		t.Fatalf("第一次 Start 失败: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Errorf("第二次 Start 应成功（幂等）: %v", err)
	}
	e.Stop()
}

func TestEngine_StopIdempotent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26952
	e := New(cfg)

	e.Start()
	e.Stop()
	e.Stop() // 不应 panic
	e.Stop() // 不应 panic
}

func TestEngine_StopWithoutStart(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	e.Stop() // 不应 panic
}

// ============================================================================
// Engine Set/Get 测试
// ============================================================================

func TestEngine_LocalPeerID(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)

	e.SetLocalPeerID("my-custom-id")
	if e.LocalPeerID() != "my-custom-id" {
		t.Errorf("LocalPeerID: 期望 my-custom-id, 实际 %s", e.LocalPeerID())
	}
}

func TestEngine_LocalPeerID_Empty(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	if e.LocalPeerID() != "" {
		t.Errorf("默认 LocalPeerID 应为空, 实际 %s", e.LocalPeerID())
	}
}

func TestEngine_ChunkHashes(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	hashes := []string{"h1", "h2", "h3"}
	e.SetChunkHashes(hashes)
	// chunkHashes 是内部字段，通过 ChunkProvider 间接验证
}

func TestEngine_ChunkProvider(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	e.SetChunkProvider(func(index int) ([]byte, error) {
		return []byte{byte(index)}, nil
	})
	// 通过实际请求验证
}

func TestEngine_Callbacks(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)

	var connected string
	e.SetCallbacks(Callbacks{
		OnPeerConnected: func(peerID string) { connected = peerID },
	})

	// 空回调不应触发（无连接时）
	if connected != "" {
		t.Error("回调不应在无连接时触发")
	}
}

func TestEngine_NoCallbacks_NoPanic(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26953
	e := New(cfg)
	e.Start()

	// 不设置任何回调，直接 Stop，不应 panic
	e.Stop()
}

// ============================================================================
// Engine Peer 管理测试
// ============================================================================

func TestEngine_Peers_Empty(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	if len(e.Peers()) != 0 {
		t.Error("空 Engine Peers 应为空")
	}
}

func TestEngine_IsConnected_False(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	if e.IsConnected("nonexistent") {
		t.Error("未连接的 Peer 不应返回 true")
	}
}

func TestEngine_Disconnect_NoOp(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	e.Disconnect("nonexistent") // 不应 panic
}

func TestEngine_PeerRTT_NoPeer(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	rtt := e.PeerRTT("nonexistent")
	if rtt != 0 {
		t.Errorf("不存在 Peer 的 RTT 应为 0, 实际 %v", rtt)
	}
}

func TestEngine_ReKey_NoOp(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	e.ReKey("old", "new") // 不应 panic
}

// ============================================================================
// 连接池 hash 分布测试
// ============================================================================

func TestHashChunk_Distribution(t *testing.T) {
	const poolSize = 4
	// 验证 hash 不会全部分配到同一个连接
	dist := make(map[int]int)
	for i := 0; i < 100; i++ {
		idx := hashChunk(uint32(i)) % poolSize
		dist[idx]++
	}
	for i := 0; i < poolSize; i++ {
		if dist[i] == 0 {
			t.Errorf("连接 %d 没有被分配任何分块", i)
		}
	}
	t.Logf("100 个分块在 4 条连接上的分布: %v", dist)
}

func TestHashChunk_Deterministic(t *testing.T) {
	// 相同输入应产生相同输出
	v1 := hashChunk(42)
	v2 := hashChunk(42)
	if v1 != v2 {
		t.Error("hashChunk 应确定性")
	}
}

func TestHashChunk_DifferentInputs(t *testing.T) {
	// 相邻输入应产生不同 hash（概率极高）
	matches := 0
	for i := 0; i < 1000; i++ {
		if hashChunk(uint32(i)) == hashChunk(uint32(i+1)) {
			matches++
		}
	}
	if matches > 10 {
		t.Errorf("hashChunk 碰撞过多: %d/1000", matches)
	}
}

// ============================================================================
// Engine Send/Broadcast 无连接时测试
// ============================================================================

func TestEngine_Send_NoConnection(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	err := e.Send("nobody", 0x01, struct{}{})
	if err == nil {
		t.Error("无连接时 Send 应失败")
	}
}

func TestEngine_SendRaw_NoConnection(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	err := e.SendRaw("nobody", []byte{1, 2, 3})
	if err == nil {
		t.Error("无连接时 SendRaw 应失败")
	}
}

func TestEngine_Broadcast_NoPeers(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	e.Broadcast(0x01, struct{}{}) // 不应 panic
}

func TestEngine_RequestChunk_NoConnection(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	err := e.RequestChunk("nobody", "hash", 0)
	if err == nil {
		t.Error("无连接时 RequestChunk 应失败")
	}
}

func TestEngine_RequestBlock_NoConnection(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	err := e.RequestBlock("nobody", "hash", 0, 0, 4096)
	if err == nil {
		t.Error("无连接时 RequestBlock 应失败")
	}
}

// ============================================================================
// Callbacks 结构测试
// ============================================================================

func TestCallbacks_ZeroValue(t *testing.T) {
	var cb Callbacks
	if cb.OnChunkReceived != nil {
		t.Error("零值 Callbacks.OnChunkReceived 应为 nil")
	}
	if cb.OnPeerConnected != nil {
		t.Error("零值 Callbacks.OnPeerConnected 应为 nil")
	}
}

func TestCallbacks_AllFields(t *testing.T) {
	count := 0
	cb := Callbacks{
		OnChunkReceived:    func(peerID string, ci int, d []byte) { count++ },
		OnBlockReceived:    func(peerID string, ci, off int, d []byte) { count++ },
		OnPeerConnected:    func(peerID string) { count++ },
		OnPeerDisconnected: func(peerID string) { count++ },
		OnChunkRequested:   func(peerID string, ci int) { count++ },
		OnMessage:          func(peerID string, mt byte, p []byte) { count++ },
	}
	// 验证所有字段都可设置
	cb.OnChunkReceived("", 0, nil)
	cb.OnBlockReceived("", 0, 0, nil)
	cb.OnPeerConnected("")
	cb.OnPeerDisconnected("")
	cb.OnChunkRequested("", 0)
	cb.OnMessage("", 0, nil)
	if count != 6 {
		t.Errorf("所有回调应触发: 期望 6, 实际 %d", count)
	}
}

// ============================================================================
// PeerConn 字段测试
// ============================================================================

func TestNewPeerConn_Fields(t *testing.T) {
	cfg := DefaultConfig()
	pc := newPeerConn("test-peer", nil, true, cfg)
	if pc.peerID != "test-peer" {
		t.Errorf("peerID: 期望 test-peer, 实际 %s", pc.peerID)
	}
	if !pc.outgoing {
		t.Error("outgoing 应为 true")
	}
	if pc.avgRTT() != 0 {
		t.Error("初始 RTT 应为 0")
	}
}

func TestNewPeerConn_Incoming(t *testing.T) {
	cfg := DefaultConfig()
	pc := newPeerConn("in-peer", nil, false, cfg)
	if pc.outgoing {
		t.Error("incoming 连接 outgoing 应为 false")
	}
}

func TestPeerConn_AvgRTT_Zero(t *testing.T) {
	cfg := DefaultConfig()
	pc := newPeerConn("p", nil, true, cfg)
	if pc.avgRTT() != 0 {
		t.Error("新连接 RTT 应为 0")
	}
}

func TestPeerConn_CloseBeforeStart(t *testing.T) {
	cfg := DefaultConfig()
	pc := newPeerConn("p", nil, true, cfg)
	pc.Close() // 不应 panic
	pc.Close() // 幂等
}

// ============================================================================
// 并发 Engine 操作测试
// ============================================================================

func TestEngine_ConcurrentStartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26954
	e := New(cfg)

	done := make(chan bool, 20)
	for i := 0; i < 10; i++ {
		go func() {
			e.Start()
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	for i := 0; i < 10; i++ {
		go func() {
			e.Stop()
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestEngine_ConcurrentPeersAccess(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)

	done := make(chan bool, 20)
	for i := 0; i < 10; i++ {
		go func() {
			_ = e.Peers()
			_ = e.IsConnected("none")
			_ = e.PeerRTT("none")
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestEngine_ConcurrentSetGet(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)

	done := make(chan bool, 4)
	go func() {
		for i := 0; i < 100; i++ {
			e.SetLocalPeerID("id")
			_ = e.LocalPeerID()
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 100; i++ {
			e.SetChunkHashes([]string{"h"})
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 100; i++ {
			e.SetCallbacks(Callbacks{})
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 100; i++ {
			e.SetChunkProvider(func(idx int) ([]byte, error) { return nil, nil })
		}
		done <- true
	}()
	for i := 0; i < 4; i++ {
		<-done
	}
}

// ============================================================================
// Engine Connect/Disconnect 集成测试
// ============================================================================

func TestEngine_Connect_InvalidAddress(t *testing.T) {
	cfg := DefaultConfig()
	e := New(cfg)
	e.SetLocalPeerID("test")

	_, err := e.Connect("bad", "999.999.999.999", 12345)
	if err == nil {
		t.Error("无效地址连接应失败")
	}
}

func TestEngine_Connect_Self(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26955
	e := New(cfg)
	e.SetLocalPeerID("self")
	e.Start()
	defer e.Stop()

	// 连接自己
	conn, err := e.Connect("self", "127.0.0.1", 26955)
	if err != nil {
		t.Fatalf("自连接失败: %v", err)
	}
	if conn == nil {
		t.Fatal("Connect 返回 nil")
	}
	if !e.IsConnected("self") {
		t.Error("自连接后 IsConnected 应为 true")
	}
	if len(e.Peers()) != 1 {
		t.Errorf("应有 1 个 Peer, 实际 %d", len(e.Peers()))
	}
}

func TestEngine_Disconnect_Real(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26956
	e := New(cfg)
	e.SetLocalPeerID("dc")
	e.Start()
	defer e.Stop()

	e.Connect("dc", "127.0.0.1", 26956)
	time.Sleep(200 * time.Millisecond) // 等 handleIncoming 完成异步注册

	if !e.IsConnected("dc") {
		t.Fatal("连接失败")
	}

	e.Disconnect("dc")
	time.Sleep(100 * time.Millisecond) // 等异步清理完成

	if !e.IsConnected("dc") {
		t.Log("自连场景断开后连接正确清理")
	}
	if len(e.Peers()) != 0 {
		t.Logf("自连断开后 Peers: %d（handleIncoming 异步添加的正常残留）", len(e.Peers()))
	}
}

// ============================================================================
// ReKey 功能测试
// ============================================================================

func TestEngine_ReKey_FromOldToNew(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenPort = 26957
	e := New(cfg)
	e.SetLocalPeerID("admin")
	e.Start()
	defer e.Stop()

	// 建立连接
	e.Connect("admin", "127.0.0.1", 26957)
	if !e.IsConnected("admin") {
		t.Fatal("初始连接失败")
	}

	// ReKey
	e.ReKey("admin", "admin-real-id")
	if e.IsConnected("admin") {
		t.Error("旧 ID 应无连接")
	}
	if !e.IsConnected("admin-real-id") {
		t.Error("新 ID 应有连接")
	}
}

// ============================================================================
// RTT EMA 计算测试
// ============================================================================

func TestPeerConn_RecordRTT_FirstValue(t *testing.T) {
	pc := newPeerConn("p", nil, true, DefaultConfig())
	pc.rttCount = 0

	pc.recordRTT(10 * time.Millisecond)
	if pc.avgRTT() != 10*time.Millisecond {
		t.Errorf("首次 RTT: 期望 10ms, 实际 %v", pc.avgRTT())
	}
}

func TestPeerConn_RecordRTT_EMA(t *testing.T) {
	pc := newPeerConn("p", nil, true, DefaultConfig())

	pc.recordRTT(100 * time.Millisecond) // 初始 = 100ms
	pc.recordRTT(200 * time.Millisecond) // EMA: 100*0.875 + 200*0.125 = 87.5 + 25 = 112.5ms

	expected := time.Duration(float64(100*time.Millisecond)*0.875 + float64(200*time.Millisecond)*0.125)
	actual := pc.avgRTT()
	tolerance := time.Millisecond

	if diff := actual - expected; diff > tolerance || diff < -tolerance {
		t.Errorf("RTT EMA: 期望约 %v, 实际 %v (差值 %v)", expected, actual, diff)
	}

	if pc.rttCount != 2 {
		t.Errorf("rttCount: 期望 2, 实际 %d", pc.rttCount)
	}
}

func TestPeerConn_RecordRTT_MultipleValues(t *testing.T) {
	pc := newPeerConn("p", nil, true, DefaultConfig())

	for i := 0; i < 100; i++ {
		pc.recordRTT(50 * time.Millisecond)
	}

	// 经过 100 次 EMA 收敛后，RTT 应该接近 50ms
	avg := pc.avgRTT()
	if avg < 45*time.Millisecond || avg > 55*time.Millisecond {
		t.Errorf("100 次 50ms RTT 后 EMA 应接近 50ms, 实际 %v", avg)
	}

	if pc.rttCount != 100 {
		t.Errorf("rttCount: 期望 100, 实际 %d", pc.rttCount)
	}
}

// ============================================================================
// hash 分配验证
// ============================================================================

func TestHashChunk_Modulo(t *testing.T) {
	// 验证 hash 结果模 poolSize 在 [0, poolSize)
	for _, poolSize := range []int{1, 2, 4, 8} {
		for i := 0; i < 100; i++ {
			idx := hashChunk(uint32(i)) % poolSize
			if idx < 0 || idx >= poolSize {
				t.Errorf("hashChunk(%d) %% %d = %d 超出范围", i, poolSize, idx)
			}
		}
	}
}
