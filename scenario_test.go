package bsfd_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cuicuisha233/bsfd/protocol"
	"github.com/cuicuisha233/bsfd/transport"
)

// ============================================================================
// 场景1: 完整文件分发 — Admin 分块 → 多个 Client 并发下载 → 组装验证
// 模拟真实 BitStorm 用例：管理员选中一个文件，客户端下载所有分块后组装
// ============================================================================

func TestScenario_FullFileDistribution(t *testing.T) {
	const (
		chunkSize  = 4096
		numChunks  = 5
		numClients = 3
	)

	// 生成模拟文件数据（每个分块内容不同）
	chunks := make([][]byte, numChunks)
	chunkHashes := make([]string, numChunks)
	for i := range chunks {
		chunks[i] = make([]byte, chunkSize)
		for j := range chunks[i] {
			chunks[i][j] = byte((i*chunkSize + j) % 256)
		}
		h := sha256.Sum256(chunks[i])
		chunkHashes[i] = hex.EncodeToString(h[:])
	}

	// === Admin (Seeder) ===
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 27000
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("admin-file-dist")
	adminEngine.SetChunkHashes(chunkHashes)

	adminStore := newTestStore(numChunks)
	for i, c := range chunks {
		adminStore.SaveChunk(i, c)
	}

	var adminReqCount int
	var adminMu sync.Mutex
	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		return adminStore.GetChunk(index)
	})
	adminEngine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			adminMu.Lock()
			adminReqCount++
			adminMu.Unlock()
			data, _ := adminStore.GetChunk(chunkIndex)
			idx := protocol.BEBytes(uint32(chunkIndex))
			payload := append(idx[:], data...)
			adminEngine.SendRaw(peerID, protocol.EncodeRaw(protocol.TypeChunkDataBin, payload))
		},
		OnPeerConnected: func(peerID string) {
			t.Logf("[Admin] 节点加入: %s", peerID)
		},
	})
	adminEngine.Start()
	defer adminEngine.Stop()

	// === N 个客户端 ===
	type clientState struct {
		engine   *transport.Engine
		store    *testStore
		received int
		mu       sync.Mutex
	}
	clients := make([]*clientState, numClients)

	for i := 0; i < numClients; i++ {
		cfg := transport.DefaultConfig()
		cfg.ListenPort = 27001 + i
		engine := transport.New(cfg)
		engine.SetLocalPeerID(fmt.Sprintf("client-%d", i+1))
		cs := &clientState{
			engine: engine,
			store:  newTestStore(numChunks),
		}
		clients[i] = cs

		engine.SetCallbacks(transport.Callbacks{
			OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
				cs.mu.Lock()
				cs.received++
				cs.mu.Unlock()
				cs.store.SaveChunk(chunkIndex, data)
			},
		})
		engine.Start()
		defer engine.Stop()

		// 连接 Admin
		engine.Connect("admin-file-dist", "127.0.0.1", 27000)
	}

	// 等待所有连接完成
	time.Sleep(500 * time.Millisecond)

	// === 每个客户端请求所有分块（通过不同连接分配） ===
	var wg sync.WaitGroup
	for i, cs := range clients {
		i, cs := i, cs
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := 0; chunk < numChunks; chunk++ {
				cs.engine.RequestChunk("admin-file-dist", chunkHashes[chunk], chunk)
			}
		}()
		t.Logf("[Client-%d] 请求 %d 个分块", i+1, numChunks)
	}
	wg.Wait()

	// 等待传输 + 重试
	for _, cs := range clients {
		for retry := 0; retry < 20 && cs.received < numChunks; retry++ {
			time.Sleep(200 * time.Millisecond)
		}
	}

	// === 验证每个客户端 ===
	for i, cs := range clients {
		cs.mu.Lock()
		rcvd := cs.received
		cs.mu.Unlock()

		t.Logf("[验证] Client-%d: 收到 %d/%d 分块", i+1, rcvd, numChunks)

		if rcvd != numChunks {
			t.Errorf("Client-%d: 只收到 %d/%d 分块", i+1, rcvd, numChunks)
		}

		if !cs.store.Complete() {
			t.Errorf("Client-%d: Store 不完整", i+1)
		}

		// 逐块验证数据完整性
		for chunk := 0; chunk < numChunks; chunk++ {
			got, err := cs.store.GetChunk(chunk)
			if err != nil {
				t.Errorf("Client-%d 缺失分块 %d", i+1, chunk)
				continue
			}
			if len(got) != len(chunks[chunk]) {
				t.Errorf("Client-%d 分块 %d 长度: 期望 %d, 实际 %d",
					i+1, chunk, len(chunks[chunk]), len(got))
				continue
			}
			for j := range got {
				if got[j] != chunks[chunk][j] {
					t.Errorf("Client-%d 分块 %d 字节 %d 不匹配", i+1, chunk, j)
					break
				}
			}
		}
	}

	t.Logf("[Admin] 总共处理了 %d 次分块请求 (理论: %d)", adminReqCount, numChunks*numClients)
	t.Logf("场景1: 完整文件分发测试通过")
}

// ============================================================================
// 场景2: Seeder 协助分发 — 有分块的节点帮助分发，减轻 Admin 负担
// ============================================================================

func TestScenario_SeederAssistance(t *testing.T) {
	chunks := [][]byte{
		[]byte("seeder-chunk-0-data-for-all-peers"),
		[]byte("seeder-chunk-1-data-for-all-peers"),
	}
	chunkHashes := []string{"sh0", "sh1"}
	numChunks := 2

	// === Admin (初始持有全部) ===
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 27010
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("admin-seeder")
	adminEngine.SetChunkHashes(chunkHashes)
	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		if index < len(chunks) {
			return chunks[index], nil
		}
		return nil, fmt.Errorf("no chunk %d", index)
	})
	adminPtr := adminEngine
	type seederCB struct {
		chunks [][]byte
	}
	sCB := seederCB{chunks}
	adminEngine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			if chunkIndex >= len(sCB.chunks) {
				return
			}
			data := sCB.chunks[chunkIndex]
			idx := protocol.BEBytes(uint32(chunkIndex))
			payload := append(idx[:], data...)
			adminPtr.SendRaw(peerID, protocol.EncodeRaw(protocol.TypeChunkDataBin, payload))
		},
	})
	adminEngine.Start()
	defer adminEngine.Stop()

	// === Seeder-1: 初始无数据，从 Admin 下载 chunk-0 ===
	seeder1Cfg := transport.DefaultConfig()
	seeder1Cfg.ListenPort = 27011
	seeder1Engine := transport.New(seeder1Cfg)
	seeder1Engine.SetLocalPeerID("seeder-1")
	seeder1Store := newTestStore(numChunks)

	var seeder1Received int
	seeder1Engine.SetCallbacks(transport.Callbacks{
		OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
			seeder1Received++
			seeder1Store.SaveChunk(chunkIndex, data)
			t.Logf("[Seeder-1] 收到分块 %d (%d 字节)", chunkIndex, len(data))
		},
	})
	seeder1Engine.Start()
	defer seeder1Engine.Stop()

	// === Client: 无数据，从 Seeder-1 下载 chunk-0，从 Admin 下载 chunk-1 ===
	clientCfg := transport.DefaultConfig()
	clientCfg.ListenPort = 27012
	clientEngine := transport.New(clientCfg)
	clientEngine.SetLocalPeerID("client-final")
	clientStore := newTestStore(numChunks)

	var clientReceived int
	var clientMu sync.Mutex
	clientEngine.SetCallbacks(transport.Callbacks{
		OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
			clientMu.Lock()
			clientReceived++
			clientMu.Unlock()
			clientStore.SaveChunk(chunkIndex, data)
			t.Logf("[Client] 收到分块 %d 来自 %s (%d 字节)", chunkIndex, peerID, len(data))
		},
	})
	clientEngine.Start()
	defer clientEngine.Stop()

	// 建立连接
	seeder1Engine.Connect("admin-seeder", "127.0.0.1", 27010)
	clientEngine.Connect("admin-seeder", "127.0.0.1", 27010)
	clientEngine.Connect("seeder-1", "127.0.0.1", 27011)
	time.Sleep(500 * time.Millisecond)

	// Seeder-1 从 Admin 下载 chunk-0
	seeder1Engine.RequestChunk("admin-seeder", "sh0", 0)
	for retry := 0; retry < 10 && seeder1Received < 1; retry++ {
		time.Sleep(200 * time.Millisecond)
	}
	if seeder1Received < 1 {
		t.Fatal("Seeder-1 未收到分块")
	}

	// 现在 Seeder-1 持有 chunk-0，应能提供给 Client
	seeder1Ptr := seeder1Engine
	seeder1Engine.SetChunkProvider(func(index int) ([]byte, error) {
		return seeder1Store.GetChunk(index)
	})
	seeder1Engine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			data, err := seeder1Store.GetChunk(chunkIndex)
			if err != nil {
				return
			}
			idx := protocol.BEBytes(uint32(chunkIndex))
			payload := append(idx[:], data...)
			seeder1Ptr.SendRaw(peerID, protocol.EncodeRaw(protocol.TypeChunkDataBin, payload))
			t.Logf("[Seeder-1] 服务分块 %d → %s", chunkIndex, peerID)
		},
	})

	// Client 从 Seeder-1 请求 chunk-0, 从 Admin 请求 chunk-1
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		clientEngine.RequestChunk("seeder-1", "sh0", 0)
	}()
	go func() {
		defer wg.Done()
		clientEngine.RequestChunk("admin-seeder", "sh1", 1)
	}()
	wg.Wait()

	for retry := 0; retry < 10 && clientReceived < 2; retry++ {
		time.Sleep(200 * time.Millisecond)
	}

	// 验证 Client 数据完整
	if !clientStore.Complete() {
		t.Errorf("Client store 不完整: missing=%v", clientStore.Missing(numChunks))
	}

	c0, _ := clientStore.GetChunk(0)
	c1, _ := clientStore.GetChunk(1)
	if string(c0) != string(chunks[0]) {
		t.Error("Client chunk-0 数据错误")
	}
	if string(c1) != string(chunks[1]) {
		t.Error("Client chunk-1 数据错误")
	}

	t.Logf("场景2: Seeder 协助分发测试通过（Client 从 2 个来源下载）")
}

// ============================================================================
// 场景3: 大文件流式传输 — 多个分块通过 Block 级请求逐步下载
// ============================================================================

func TestScenario_LargeFileStreaming(t *testing.T) {
	const (
		chunkSize  = 65536  // 64KB per chunk
		blockSize  = 16384  // 16KB per block
		numChunks  = 4
	)

	// 生成文件数据
	fileData := make([][]byte, numChunks)
	for i := range fileData {
		fileData[i] = make([]byte, chunkSize)
		for j := range fileData[i] {
			fileData[i][j] = byte((i*chunkSize + j) % 251)
		}
	}

	// Admin
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 27020
	adminCfg.BlockSize = blockSize
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("admin-stream")
	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		if index < len(fileData) {
			return fileData[index], nil
		}
		return nil, fmt.Errorf("no chunk")
	})
	adminEngine.Start()
	defer adminEngine.Stop()

	// Client
	clientCfg := transport.DefaultConfig()
	clientCfg.ListenPort = 27021
	clientCfg.BlockSize = blockSize
	clientEngine := transport.New(clientCfg)
	clientEngine.SetLocalPeerID("client-stream")

	// 组装缓冲区
	assembled := make([][]byte, numChunks)
	for i := range assembled {
		assembled[i] = make([]byte, chunkSize)
	}
	var mu sync.Mutex
	var totalBlocks int

	clientEngine.SetCallbacks(transport.Callbacks{
		OnBlockReceived: func(peerID string, chunkIndex int, blockOffset int, data []byte) {
			mu.Lock()
			copy(assembled[chunkIndex][blockOffset:], data)
			totalBlocks++
			mu.Unlock()
		},
	})
	clientEngine.Start()
	defer clientEngine.Stop()

	clientEngine.Connect("admin-stream", "127.0.0.1", 27020)
	time.Sleep(500 * time.Millisecond)

	// 按 block 下载所有 chunk
	blocksPerChunk := chunkSize / blockSize
	var wg sync.WaitGroup
	for chunk := 0; chunk < numChunks; chunk++ {
		for blk := 0; blk < blocksPerChunk; blk++ {
			wg.Add(1)
			go func(chunk, offset int) {
				defer wg.Done()
				clientEngine.RequestBlock("admin-stream", "hash", chunk, offset, blockSize)
			}(chunk, blk*blockSize)
		}
	}
	wg.Wait()

	// 等待所有 block 到达
	for retry := 0; retry < 30 && totalBlocks < numChunks*blocksPerChunk; retry++ {
		time.Sleep(200 * time.Millisecond)
	}

	expectedBlocks := numChunks * blocksPerChunk
	t.Logf("收到 %d/%d blocks", totalBlocks, expectedBlocks)

	if totalBlocks < expectedBlocks/2 {
		t.Fatalf("严重丢失: 仅收到 %d/%d blocks", totalBlocks, expectedBlocks)
	}

	// 验证每个 chunk 的数据
	for chunk := 0; chunk < numChunks; chunk++ {
		for j := 0; j < chunkSize; j++ {
			if assembled[chunk][j] != fileData[chunk][j] {
				t.Errorf("Chunk %d, 字节 %d 不匹配: 期望 %d, 实际 %d",
					chunk, j, fileData[chunk][j], assembled[chunk][j])
				break
			}
		}
	}

	t.Logf("场景3: 大文件流式传输测试通过")
}

// ============================================================================
// 场景4: 断连恢复 — 传输中断后重连继续下载未完成的分块
// ============================================================================

func TestScenario_DisconnectRecovery(t *testing.T) {
	chunks := [][]byte{
		[]byte("reconnect-chunk-0-test-data-for-recovery"),
		[]byte("reconnect-chunk-1-test-data-for-recovery"),
		[]byte("reconnect-chunk-2-test-data-for-recovery"),
	}
	numChunks := 3

	// Admin
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 27030
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("admin-reconnect")
	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		if index < len(chunks) {
			return chunks[index], nil
		}
		return nil, fmt.Errorf("no chunk")
	})

	adminPtr := adminEngine
	adminChunks := chunks
	adminEngine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			if chunkIndex >= len(adminChunks) {
				return
			}
			data := adminChunks[chunkIndex]
			idx := protocol.BEBytes(uint32(chunkIndex))
			payload := append(idx[:], data...)
			adminPtr.SendRaw(peerID, protocol.EncodeRaw(protocol.TypeChunkDataBin, payload))
		},
	})
	adminEngine.Start()
	defer adminEngine.Stop()

	// Client
	clientCfg := transport.DefaultConfig()
	clientCfg.ListenPort = 27031
	clientEngine := transport.New(clientCfg)
	clientEngine.SetLocalPeerID("client-reconnect")
	clientStore := newTestStore(numChunks)

	var received int
	var recvMu sync.Mutex
	clientEngine.SetCallbacks(transport.Callbacks{
		OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
			recvMu.Lock()
			received++
			recvMu.Unlock()
			clientStore.SaveChunk(chunkIndex, data)
		},
	})
	clientEngine.Start()
	defer clientEngine.Stop()

	// 第一轮：连接并下载 chunk-0
	clientEngine.Connect("admin-reconnect", "127.0.0.1", 27030)
	time.Sleep(400 * time.Millisecond)

	clientEngine.RequestChunk("admin-reconnect", "h0", 0)
	for retry := 0; retry < 10 && received < 1; retry++ {
		time.Sleep(200 * time.Millisecond)
	}
	if received < 1 {
		t.Fatal("第一轮: 未收到 chunk-0")
	}
	t.Log("第一轮完成: chunk-0 已下载")

	// 断开连接
	clientEngine.Disconnect("admin-reconnect")
	time.Sleep(300 * time.Millisecond)

	// 第二轮：重连并下载剩余 chunk-1, chunk-2
	clientEngine.Connect("admin-reconnect", "127.0.0.1", 27030)
	time.Sleep(500 * time.Millisecond)

	if !clientEngine.IsConnected("admin-reconnect") {
		t.Fatal("重连失败")
	}

	// 请求剩余分块
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		clientEngine.RequestChunk("admin-reconnect", "h1", 1)
	}()
	go func() {
		defer wg.Done()
		clientEngine.RequestChunk("admin-reconnect", "h2", 2)
	}()
	wg.Wait()

	for retry := 0; retry < 10 && received < 3; retry++ {
		time.Sleep(200 * time.Millisecond)
	}

	// 验证
	if !clientStore.Complete() {
		t.Errorf("Store 不完整: 收到 %d/3, missing=%v", received, clientStore.Missing(numChunks))
	}
	for i, expected := range chunks {
		got, _ := clientStore.GetChunk(i)
		if string(got) != string(expected) {
			t.Errorf("Chunk %d 数据错误", i)
		}
	}

	t.Logf("场景4: 断连恢复测试通过")
}

// ============================================================================
// 场景5: 节点动态加入 — 传输中途新节点加入，立即参与分发
// ============================================================================

func TestScenario_LateJoiner(t *testing.T) {
	chunks := [][]byte{
		[]byte("late-joiner-chunk-0"),
		[]byte("late-joiner-chunk-1"),
	}

	// Admin
	adminCfg := transport.DefaultConfig()
	adminCfg.ListenPort = 27040
	adminEngine := transport.New(adminCfg)
	adminEngine.SetLocalPeerID("admin-late")

	adminPtr := adminEngine
	adminChunks := chunks
	adminEngine.SetChunkProvider(func(index int) ([]byte, error) {
		if index < len(adminChunks) {
			return adminChunks[index], nil
		}
		return nil, fmt.Errorf("no chunk")
	})
	adminEngine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			if chunkIndex >= len(adminChunks) {
				return
			}
			data := adminChunks[chunkIndex]
			idx := protocol.BEBytes(uint32(chunkIndex))
			payload := append(idx[:], data...)
			adminPtr.SendRaw(peerID, protocol.EncodeRaw(protocol.TypeChunkDataBin, payload))
		},
	})
	adminEngine.Start()
	defer adminEngine.Stop()

	// Client-1 (早期加入)
	c1Cfg := transport.DefaultConfig()
	c1Cfg.ListenPort = 27041
	c1Engine := transport.New(c1Cfg)
	c1Engine.SetLocalPeerID("client-early")
	c1Store := newTestStore(2)
	c1Engine.SetCallbacks(transport.Callbacks{
		OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
			c1Store.SaveChunk(chunkIndex, data)
		},
	})
	c1Engine.Start()
	defer c1Engine.Stop()

	c1Engine.Connect("admin-late", "127.0.0.1", 27040)
	time.Sleep(400 * time.Millisecond)

	// Client-1 下载 chunk-0
	c1Engine.RequestChunk("admin-late", "h0", 0)
	time.Sleep(500 * time.Millisecond)
	if !c1Store.HasChunk(0) {
		t.Fatal("Client-1 未收到 chunk-0")
	}

	// Client-2 中途加入
	c2Cfg := transport.DefaultConfig()
	c2Cfg.ListenPort = 27042
	c2Engine := transport.New(c2Cfg)
	c2Engine.SetLocalPeerID("client-late")
	c2Store := newTestStore(2)
	c2Engine.SetCallbacks(transport.Callbacks{
		OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
			c2Store.SaveChunk(chunkIndex, data)
		},
	})
	c2Engine.Start()
	defer c2Engine.Stop()

	c2Engine.Connect("admin-late", "127.0.0.1", 27040)
	time.Sleep(400 * time.Millisecond)

	// Client-2 请求两个分块：chunk-0 可能从 Client-1 获取（未来实现），现在从 Admin
	c2Engine.RequestChunk("admin-late", "h0", 0)
	c2Engine.RequestChunk("admin-late", "h1", 1)

	time.Sleep(500 * time.Millisecond)

	// Client-1 也请求 chunk-1
	c1Engine.RequestChunk("admin-late", "h1", 1)
	time.Sleep(500 * time.Millisecond)

	// 验证两者都完整
	if !c1Store.Complete() {
		t.Error("Client-1 Store 不完整")
	}
	if !c2Store.Complete() {
		t.Error("Client-2 Store 不完整")
	}

	t.Logf("场景5: 节点动态加入测试通过")
}
