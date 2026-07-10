package discovery

import (
	"net"
	"testing"
	"time"
)

// ============================================================================
// Config 测试
// ============================================================================

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.UDPPort != 26931 {
		t.Errorf("UDPPort: 期望 26931, 实际 %d", cfg.UDPPort)
	}
	if cfg.BroadcastInterval != 2*time.Second {
		t.Errorf("BroadcastInterval: 期望 2s, 实际 %v", cfg.BroadcastInterval)
	}
	if cfg.ReadBufferSize != 65536 {
		t.Errorf("ReadBufferSize: 期望 65536, 实际 %d", cfg.ReadBufferSize)
	}
}

// ============================================================================
// BroadcastAddr 测试
// ============================================================================

func TestBroadcastAddr_24Prefix(t *testing.T) {
	_, net24, _ := net.ParseCIDR("192.168.1.0/24")
	result := BroadcastAddr(net24)
	expected := "192.168.1.255"
	if result == nil || result.String() != expected {
		t.Errorf("/24 广播地址: 期望 %s, 实际 %v", expected, result)
	}
}

func TestBroadcastAddr_16Prefix(t *testing.T) {
	_, net16, _ := net.ParseCIDR("10.0.0.0/16")
	result := BroadcastAddr(net16)
	expected := "10.0.255.255"
	if result == nil || result.String() != expected {
		t.Errorf("/16 广播地址: 期望 %s, 实际 %v", expected, result)
	}
}

func TestBroadcastAddr_8Prefix(t *testing.T) {
	_, net8, _ := net.ParseCIDR("10.0.0.0/8")
	result := BroadcastAddr(net8)
	expected := "10.255.255.255"
	if result == nil || result.String() != expected {
		t.Errorf("/8 广播地址: 期望 %s, 实际 %v", expected, result)
	}
}

func TestBroadcastAddr_32Prefix(t *testing.T) {
	// /32 没有广播地址（只有一个 IP）
	_, net32, _ := net.ParseCIDR("192.168.1.1/32")
	result := BroadcastAddr(net32)
	if result == nil {
		t.Error("/32 广播地址不应为 nil")
	}
}

func TestBroadcastAddr_NilInput(t *testing.T) {
	result := BroadcastAddr(nil)
	if result != nil {
		t.Error("nil 输入应返回 nil")
	}
}

func TestBroadcastAddr_IPv6(t *testing.T) {
	_, netv6, _ := net.ParseCIDR("2001:db8::/32")
	result := BroadcastAddr(netv6)
	if result != nil {
		t.Error("IPv6 应返回 nil（仅支持 IPv4）")
	}
}

// ============================================================================
// LocalIPs / IsLocalIP / Hostname 测试
// ============================================================================

func TestLocalIPs_NotEmpty(t *testing.T) {
	ips := LocalIPs()
	if len(ips) == 0 {
		t.Skip("未检测到非回环 IP，跳过（可能是无网络环境）")
	}
	for _, ip := range ips {
		if net.ParseIP(ip).To4() == nil {
			t.Errorf("非 IPv4 地址: %s", ip)
		}
	}
	t.Logf("本地 IP: %v", ips)
}

func TestLocalIPs_Caching(t *testing.T) {
	// 第一次调用
	ips1 := LocalIPs()
	// 立即第二次调用应该走缓存
	ips2 := LocalIPs()
	if len(ips1) != len(ips2) {
		t.Errorf("缓存不一致: %d vs %d", len(ips1), len(ips2))
	}
}

func TestIsLocalIP_Loopback(t *testing.T) {
	if IsLocalIP(net.ParseIP("127.0.0.1")) {
		t.Log("127.0.0.1 被识别为本地（取决于网卡配置）")
	}
}

func TestIsLocalIP_External(t *testing.T) {
	if IsLocalIP(net.ParseIP("203.0.113.1")) {
		t.Error("外网 IP 不应被识别为本地")
	}
}

func TestIsLocalIP_IPv6(t *testing.T) {
	// 我们的实现只检查 IPv4
	if IsLocalIP(net.ParseIP("::1")) {
		t.Log("::1 被识别为本地")
	}
}

func TestHostname_NotEmpty(t *testing.T) {
	h := Hostname()
	if h == "" {
		t.Error("Hostname 不应为空")
	}
	if h == "unknown" {
		t.Log("无法获取主机名，使用 'unknown' 作为降级值")
	} else {
		t.Logf("主机名: %s", h)
	}
}

// ============================================================================
// bufferReader 测试
// ============================================================================

func TestBufferReader_ExactRead(t *testing.T) {
	data := []byte("hello world")
	reader := &bufferReader{buf: data}

	buf := make([]byte, 11)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if n != 11 {
		t.Errorf("读取长度: 期望 11, 实际 %d", n)
	}
	if string(buf) != "hello world" {
		t.Errorf("内容: 期望 %q, 实际 %q", "hello world", string(buf))
	}
}

func TestBufferReader_PartialRead(t *testing.T) {
	data := []byte("abcdef")
	reader := &bufferReader{buf: data}

	// 第一次读 4 字节
	buf1 := make([]byte, 4)
	n, err := reader.Read(buf1)
	if err != nil || n != 4 || string(buf1) != "abcd" {
		t.Fatalf("第一次读取失败: n=%d err=%v", n, err)
	}

	// 第二次读剩余 2 字节
	buf2 := make([]byte, 4)
	n, err = reader.Read(buf2)
	if err != nil || n != 2 || string(buf2[:2]) != "ef" {
		t.Fatalf("第二次读取失败: n=%d err=%v", n, err)
	}
}

func TestBufferReader_ReadPastEnd(t *testing.T) {
	data := []byte("abc")
	reader := &bufferReader{buf: data}

	// 读 3 字节
	buf1 := make([]byte, 3)
	reader.Read(buf1)

	// 再读应该报错
	buf2 := make([]byte, 1)
	n, err := reader.Read(buf2)
	if err == nil {
		t.Error("超过末尾应报错")
	}
	if n != 0 {
		t.Errorf("超过末尾应读 0 字节, 实际 %d", n)
	}
}

func TestBufferReader_Empty(t *testing.T) {
	reader := &bufferReader{buf: []byte{}}
	buf := make([]byte, 1)
	n, err := reader.Read(buf)
	if err == nil {
		t.Error("空 buffer 应报错")
	}
	if n != 0 {
		t.Errorf("空 buffer 读: 期望 0, 实际 %d", n)
	}
}

func TestBufferReader_LargeBuffer(t *testing.T) {
	data := []byte{1, 2, 3}
	reader := &bufferReader{buf: data}
	buf := make([]byte, 1024) // 比数据大很多
	n, err := reader.Read(buf)
	if err != nil || n != 3 {
		t.Errorf("大缓冲区读: n=%d err=%v", n, err)
	}
}

// ============================================================================
// Broadcaster 测试
// ============================================================================

func TestNewBroadcaster(t *testing.T) {
	cfg := DefaultConfig()
	b := NewBroadcaster(cfg, "peer-1", "test", "192.168.0.1", 8080, "admin", 10, nil, nil)
	if b == nil {
		t.Fatal("NewBroadcaster 返回 nil")
	}
	if b.peerID != "peer-1" {
		t.Errorf("peerID: 期望 peer-1, 实际 %s", b.peerID)
	}
	if b.role != "admin" {
		t.Errorf("role: 期望 admin, 实际 %s", b.role)
	}
	if b.totalChunks != 10 {
		t.Errorf("totalChunks: 期望 10, 实际 %d", b.totalChunks)
	}
}

func TestBroadcaster_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastInterval = 100 * time.Millisecond
	b := NewBroadcaster(cfg, "peer-test", "tester", "127.0.0.1", 26932, "client", 0, nil, nil)

	// 如果有可用网卡接口，应该能启动
	if err := b.Start(); err != nil {
		t.Logf("Broadcaster 启动失败（可能无合适网卡）: %v", err)
		t.Skip("跳过：无可用网卡接口")
	}

	// 等一个广播周期
	time.Sleep(150 * time.Millisecond)

	// 停止（幂等）
	b.Stop()
	b.Stop() // 第二次不应 panic
}

func TestBroadcaster_WithBitmap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastInterval = 50 * time.Millisecond
	bitmap := []byte{0xFF, 0x0F, 0x00}
	hashes := []string{"hash1", "hash2", "hash3"}

	b := NewBroadcaster(cfg, "peer-bm", "bitmap-node", "10.0.0.1", 8080, "admin", 24,
		func() []byte { return bitmap },
		func() []string { return hashes },
	)
	if b == nil {
		t.Fatal("NewBroadcaster 返回 nil")
	}
	b.Stop()
}

func TestBroadcaster_ClientRole(t *testing.T) {
	cfg := DefaultConfig()
	b := NewBroadcaster(cfg, "client-1", "client-node", "10.0.0.2", 9090, "client", 0, nil, nil)
	if b.role != "client" {
		t.Errorf("role: 期望 client, 实际 %s", b.role)
	}
}

// ============================================================================
// Listener 测试
// ============================================================================

func TestNewListener(t *testing.T) {
	cfg := DefaultConfig()
	l := NewListener(cfg)
	if l == nil {
		t.Fatal("NewListener 返回 nil")
	}
	if len(l.Peers()) != 0 {
		t.Error("新 Listener 应无 Peer")
	}
	if len(l.Admins()) != 0 {
		t.Error("新 Listener 应无 Admin")
	}
}

func TestListener_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.UDPPort = 26990 // 避免和其他测试冲突
	l := NewListener(cfg)

	if err := l.Start(); err != nil {
		// 可能端口被占用或权限不足
		t.Skipf("Listener 启动失败: %v", err)
	}

	// 停止（幂等）
	l.Stop()
	l.Stop() // 第二次不应 panic

	if len(l.Peers()) != 0 {
		t.Error("空 Listener Peers 应为空")
	}
}

func TestListener_EmptyPeers(t *testing.T) {
	cfg := DefaultConfig()
	l := NewListener(cfg)
	if len(l.Peers()) != 0 {
		t.Error("未启动的 Listener Peers 应为空")
	}
	if len(l.Admins()) != 0 {
		t.Error("未启动的 Listener Admins 应为空")
	}
}

func TestListener_Callbacks(t *testing.T) {
	cfg := DefaultConfig()
	l := NewListener(cfg)

	var discovered bool
	l.OnDiscover(func(admin DiscoveredAdmin) {
		discovered = true
	})

	var peerUpdated bool
	l.OnPeer(func(peer DiscoveredPeer) {
		peerUpdated = true
	})

	// 回调不应在启动前触发
	if discovered {
		t.Error("回调不应在启动前触发")
	}
	if peerUpdated {
		t.Error("回调不应在启动前触发")
	}
}

// ============================================================================
// DiscovertedAdmin / DiscoveredPeer 字段测试
// ============================================================================

func TestDiscoveredAdmin_Fields(t *testing.T) {
	admin := DiscoveredAdmin{
		Name:    "管理端",
		IP:      "192.168.1.1",
		TCPPort: 26932,
	}
	if admin.Name != "管理端" {
		t.Errorf("Name: 期望 管理端, 实际 %s", admin.Name)
	}
	if admin.IP != "192.168.1.1" {
		t.Errorf("IP: 期望 192.168.1.1, 实际 %s", admin.IP)
	}
	if admin.TCPPort != 26932 {
		t.Errorf("TCPPort: 期望 26932, 实际 %d", admin.TCPPort)
	}
}

func TestDiscoveredPeer_Fields(t *testing.T) {
	peer := DiscoveredPeer{
		PeerID:          "peer-abc",
		Name:            "节点ABC",
		IP:              "10.0.0.5",
		TCPPort:         8080,
		Role:            "client",
		ChunkBitmap:     []byte{0xAA, 0xBB},
		TotalChunks:     16,
		PreloadedHashes: []string{"h1", "h2"},
	}
	if peer.PeerID != "peer-abc" {
		t.Errorf("PeerID 不匹配")
	}
	if peer.Role != "client" {
		t.Errorf("Role 不匹配")
	}
	if peer.TotalChunks != 16 {
		t.Errorf("TotalChunks: 期望 16, 实际 %d", peer.TotalChunks)
	}
	if len(peer.PreloadedHashes) != 2 {
		t.Errorf("PreloadedHashes 长度: 期望 2, 实际 %d", len(peer.PreloadedHashes))
	}
}

// ============================================================================
// 网络工具边界测试
// ============================================================================

func TestBroadcastAddr_KnownSubnets(t *testing.T) {
	tests := []struct {
		cidr     string
		expected string
	}{
		{"192.168.0.0/16", "192.168.255.255"},
		{"172.16.0.0/12", "172.31.255.255"},
		{"10.10.10.0/24", "10.10.10.255"},
		{"0.0.0.0/0", "255.255.255.255"},
	}
	for _, tc := range tests {
		_, ipNet, err := net.ParseCIDR(tc.cidr)
		if err != nil {
			t.Fatalf("解析 CIDR %s 失败: %v", tc.cidr, err)
		}
		result := BroadcastAddr(ipNet)
		if result == nil {
			t.Errorf("%s 广播地址为 nil", tc.cidr)
			continue
		}
		if result.String() != tc.expected {
			t.Errorf("%s: 期望 %s, 实际 %s", tc.cidr, tc.expected, result.String())
		}
	}
}

func TestBroadcastAddr_InvalidMask(t *testing.T) {
	// 构造一个 IPv4 IP 和非法 mask
	ip := net.ParseIP("192.168.1.1")
	mask := net.IPMask{0xFF, 0xFF} // 只有 2 字节
	ipNet := &net.IPNet{IP: ip, Mask: mask}
	result := BroadcastAddr(ipNet)
	if result != nil {
		t.Error("非法 mask 应返回 nil")
	}
}

func TestIsLocalIP_AllLocalIPs(t *testing.T) {
	ips := LocalIPs()
	for _, ipStr := range ips {
		if !IsLocalIP(net.ParseIP(ipStr)) {
			t.Errorf("LocalIPs 返回的 IP %s 未被 IsLocalIP 识别", ipStr)
		}
	}
}
