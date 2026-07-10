# BSFD — BitStorm 文件分发协议引擎

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

轻量级、可嵌入的 P2P 文件分发协议引擎，专为高吞吐局域网环境设计。
**零外部依赖**，仅使用 Go 标准库，可嵌入任何 Go 应用。

## 设计哲学

BSFD 是一个**协议引擎**，不是应用。它没有 main 函数，不绑定 GUI 框架，不假设文件系统结构。

宿主应用通过三个接口对接：
1. **回调函数** — 引擎通过回调通知宿主事件（新连接、收到分块、节点断开等）
2. **Storage 接口** — 宿主实现分块读写，引擎不关心文件如何存储
3. **Config 结构** — 所有端口、超时、缓冲区大小均可配置

```
┌────────────────────────────────────┐
│           你的应用                   │
│  ┌──────────────────────────────┐  │
│  │   实现 Storage 接口           │  │
│  ├──────────────────────────────┤  │
│  │   注册回调函数                  │  │
│  ├──────┬───────┬───────┬──────┥  │
│  │ UDP  │  TCP  │ 帧协议 │ 种子  │  │
│  │ 发现  │ 传输   │ 编解码  │ 格式  │  │
│  └──────┴───────┴───────┴──────┘  │
│          BSFD 协议引擎              │
└────────────────────────────────────┘
```

## 功能特性

| 特性 | 说明 |
|------|------|
| **UDP 自动发现** | 周期性心跳广播，客户端零配置发现管理端 |
| **TCP 连接池** | 每节点 4 条连接，`hash(chunkIndex)` 分配负载 |
| **块级流水线** | 16KB 块传输，多节点并发下载同一分块 |
| **RTT 流控** | EMA（α=0.125）平滑往返时延，自动限制在途请求 |
| **种子文件** | `.bsfd` 格式导出/导入，支持预部署哈希匹配 |
| **零依赖** | `go.mod` 无 `require`，仅 Go 标准库 |

## 模块架构

```
bsfd/
├── protocol/    帧格式定义、13 种消息类型、JSON/二进制编解码
├── discovery/   UDP 心跳广播、监听器、节点注册表、网络工具函数
├── transport/   TCP 引擎、PeerConn、连接池、RTT EMA、块调度
└── storage/     分块存储接口（Store 接口，由宿主应用实现）
```

## 线缆格式 (v1)

```
┌──────────┬──────┬───────────────┬─────────────┐
│  魔数 4B  │ 类型 1B │ 负载长度 4B(BE) │  负载 N 字节  │
│  "BSFD"  │ 0x01~FF │   uint32      │ JSON/Binary │
└──────────┴──────┴───────────────┴─────────────┘
  偏移 0      偏移 4     偏移 5             偏移 9
```

| 字段 | 偏移 | 字节数 | 说明 |
|------|------|--------|------|
| 魔数 | 0 | 4 | 固定 `BSFD`（0x42 0x53 0x46 0x44） |
| 类型 | 4 | 1 | 标识消息类别 |
| 负载长度 | 5 | 4 | 大端序 uint32，最大 32 MiB |
| 负载 | 9 | N | JSON 对象或原始二进制 |

**帧头常量**：`HeaderLen=9`，`MaxPayload=32MiB`，`Version=1`

## 消息类型

### 发现层 (0x01)
| 值 | 常量 | 说明 | 方向 | 格式 |
|----|------|------|------|------|
| 0x01 | `TypeHeartbeat` | UDP 心跳广播，携带 bitmap 和预加载哈希 | 双向 | JSON |

### 连接层 (0x02-0x04)
| 值 | 常量 | 说明 | 方向 | 格式 |
|----|------|------|------|------|
| 0x02 | `TypeHello` | TCP 握手，报告 PeerID | 双向 | JSON |
| 0x03 | `TypeFileInfo` | 文件元信息（名称、大小、分块数） | 管理端→客户端 | JSON |
| 0x04 | `TypeStartDist` | 通知开始分发 | 管理端→客户端 | JSON |

### 传输层 (0x10-0x14)
| 值 | 常量 | 说明 | 方向 | 格式 |
|----|------|------|------|------|
| 0x10 | `TypeChunkRequest` | 请求一个完整分块 | 节点互发 | JSON |
| 0x11 | `TypeChunkData` | JSON 封装的分块数据 | 节点互发 | JSON |
| 0x12 | `TypeChunkDataBin` | 二进制分块 `[4B 索引 BE][原始数据]` | 节点互发 | Binary |
| 0x13 | `TypeBlockRequest` | 请求分块内的一段偏移范围 | 节点互发 | JSON |
| 0x14 | `TypeBlockDataBin` | 二进制块 `[4B 偏移 BE][原始数据]` | 节点互发 | Binary |

### 控制层 (0x30-0x31)
| 值 | 常量 | 说明 | 方向 | 格式 |
|----|------|------|------|------|
| 0x30 | `TypeCancel` | 取消分发 | 管理端→客户端 | JSON |
| 0x31 | `TypeFinish` | 分发完成通知 | 管理端→客户端 | JSON |

### 生命周期 (0xFE-0xFF)
| 值 | 常量 | 说明 | 方向 | 格式 |
|----|------|------|------|------|
| 0xFE | `TypeKeepAlive` | 保活心跳 | 双向 | JSON |
| 0xFF | `TypeDisconnect` | 正常断开 | 双向 | JSON |

## 安装

```bash
go get github.com/cuicuisha233/bsfd
```

## 快速开始

### 管理端 — 广播 + 分块提供

```go
package main

import (
	"log"

	"github.com/cuicuisha233/bsfd/discovery"
	"github.com/cuicuisha233/bsfd/protocol"
	"github.com/cuicuisha233/bsfd/transport"
)

func main() {
	// 1. UDP 广播：让客户端能发现我们
	discCfg := discovery.DefaultConfig()
	broadcaster := discovery.NewBroadcaster(
		discCfg, "admin-001", "我的管理端",
		discovery.LocalIPs()[0], 26932, "admin", 10,
		func() []byte { return myBitmap() },          // bitmap 生成函数
		func() []string { return myPreloadedHashes() }, // 预加载哈希
	)
	broadcaster.Start()
	defer broadcaster.Stop()

	// 2. TCP 引擎：接受客户端连接并分发分块
	cfg := transport.DefaultConfig()
	engine := transport.New(cfg)
	engine.SetLocalPeerID("admin-001")
	engine.SetChunkProvider(func(index int) ([]byte, error) {
		return loadChunkFromDisk(index)
	})
	engine.SetCallbacks(transport.Callbacks{
		OnChunkRequested: func(peerID string, chunkIndex int) {
			// 引擎只通知变量，宿主负责发送数据
			data, _ := loadChunkFromDisk(chunkIndex)
			idx := protocol.BEBytes(uint32(chunkIndex))
			payload := append(idx[:], data...)
			engine.SendRaw(peerID, protocol.EncodeRaw(protocol.TypeChunkDataBin, payload))
			log.Printf("发送分块 %d → %s (%d 字节)", chunkIndex, peerID, len(data))
		},
		OnPeerConnected: func(peerID string) {
			log.Printf("节点加入: %s", peerID)
		},
		OnPeerDisconnected: func(peerID string) {
			log.Printf("节点离开: %s", peerID)
		},
	})
	engine.Start()
	defer engine.Stop()
}
```

### 客户端 — 发现 + 下载

```go
func runClient() {
	// 1. UDP 监听：发现管理端
	discCfg := discovery.DefaultConfig()
	listener := discovery.NewListener(discCfg)
	listener.OnDiscover(func(admin discovery.DiscoveredAdmin) {
		log.Printf("发现管理端: %s @ %s:%d", admin.Name, admin.IP, admin.TCPPort)
		// 自动连接
		engine.Connect("admin-001", admin.IP, admin.TCPPort)
	})
	listener.Start()
	defer listener.Stop()

	// 2. TCP 引擎：接收分块数据
	cfg := transport.DefaultConfig()
	engine := transport.New(cfg)
	engine.SetLocalPeerID("client-001")
	engine.SetCallbacks(transport.Callbacks{
		OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
			log.Printf("收到分块 %d (%d 字节)", chunkIndex, len(data))
			saveChunkToDisk(chunkIndex, data)
		},
	})
	engine.Start()
	defer engine.Stop()

	// 3. 请求所有分块
	for i := 0; i < totalChunks; i++ {
		engine.RequestChunk("admin-001", fileHash, i)
	}
}
```

### 流式块传输 — 大文件按 Block 下载

```go
engine.SetCallbacks(transport.Callbacks{
	OnBlockReceived: func(peerID string, chunkIndex int, blockOffset int, data []byte) {
		// 直接将 block 写入文件对应偏移位置
		writeAtOffset(chunkIndex*chunkSize + blockOffset, data)
	},
})

// 请求分块 0 的所有 block
for offset := 0; offset < chunkSize; offset += blockSize {
	engine.RequestBlock("admin-001", "hash", 0, offset, blockSize)
}
```

### 种子文件 — 预部署校验

```go
import "github.com/cuicuisha233/bsfd/protocol"

// 导出种子
seed := protocol.SeedFile{
	FileName:   "ubuntu.iso",
	FileSize:   5368709120,
	ChunkSize:  1 << 20,
	ChunkHash:  []string{"hash0", "hash1", "..."},
}
data, _ := json.MarshalIndent(seed, "", "  ")
os.WriteFile("ubuntu.iso.bsfd", data, 0644)

// 导入种子并预加载
seedData, _ := os.ReadFile("ubuntu.iso.bsfd")
var seed protocol.SeedFile
json.Unmarshal(seedData, &seed)
engine.SetChunkHashes(seed.ChunkHash)
```

## Storage 接口

宿主应用实现此接口以集成存储：

```go
type Store interface {
	SaveChunk(index int, data []byte) error
	GetChunk(index int) ([]byte, error)
	HasChunk(index int) bool
	Missing(total int) []int
	Complete() bool
	Total() int
}
```

## API 参考

### transport.Engine

| 方法 | 说明 |
|------|------|
| `New(cfg Config) *Engine` | 创建引擎 |
| `Start() error` | 启动 TCP 监听 |
| `Stop()` | 停止引擎 |
| `Connect(peerID, ip string, port int)` | 连接远程节点 |
| `Disconnect(peerID string)` | 断开节点 |
| `Send(peerID string, msgType byte, msg any) error` | 发送 JSON 消息 |
| `SendRaw(peerID string, data []byte) error` | 发送原始帧 |
| `Broadcast(msgType byte, msg any)` | 向所有节点广播 |
| `RequestChunk(peerID, fileHash string, index int) error` | 请求完整分块 |
| `RequestBlock(peerID, fileHash string, chunkIndex, offset, size int) error` | 请求块 |
| `Peers() []string` | 返回已连接节点列表 |
| `IsConnected(peerID string) bool` | 检查是否已连接 |
| `PeerRTT(peerID string) time.Duration` | 获取节点对应的 RTT |
| `ReKey(oldID, newID string)` | 重命名节点 |
| `SetLocalPeerID(id string)` | 设置本节点 ID |
| `LocalPeerID() string` | 获取本节点 ID |
| `SetChunkProvider(fn func(int) ([]byte, error))` | 设置分块提供器 |
| `SetChunkHashes(hashes []string)` | 设置分块哈希列表 |
| `SetCallbacks(cb Callbacks)` | 注册回调 |

### transport.Config 默认值

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ListenPort` | 26932 | TCP 监听端口 |
| `ConnPoolSize` | 4 | 每节点 TCP 连接数 |
| `ConnTimeout` | 10s | 连接超时 |
| `HeartbeatInterval` | 10s | 保活心跳间隔 |
| `HeartbeatTimeout` | 30s | 心跳超时阈值 |
| `MaxInflightRequests` | 8 | 单连接最大在途请求 |
| `BlockSize` | 16384 | 块大小 (16KB) |
| `MaxRetries` | 3 | 最大重试次数 |

### discovery

| 函数/方法 | 说明 |
|-----------|------|
| `DefaultConfig() Config` | 返回默认发现配置 |
| `NewBroadcaster(cfg, peerID, name, ip, port, role, total, bitmapFn, hashesFn)` | 创建广播器 |
| `Broadcaster.Start()` | 开始向所有接口广播 |
| `Broadcaster.Stop()` | 停止广播（幂等） |
| `NewListener(cfg)` | 创建监听器 |
| `Listener.OnDiscover(fn)` | 注册发现管理端回调 |
| `Listener.OnPeer(fn)` | 注册 peer 更新回调 |
| `Listener.Start()` | 开始监听 UDP |
| `Listener.Stop()` | 停止监听（幂等） |
| `Listener.Admins() []DiscoveredAdmin` | 返回所有已发现管理端 |
| `Listener.Peers() []DiscoveredPeer` | 返回所有已发现节点 |
| `LocalIPs() []string` | 获取本机非回环 IPv4 地址 |
| `Hostname() string` | 获取主机名 |
| `IsLocalIP(ip net.IP) bool` | 判断是否本机地址 |
| `BroadcastAddr(ipNet *net.IPNet) net.IP` | 计算广播地址 |

### protocol 编解码

| 函数 | 说明 |
|------|------|
| `Encode(msgType byte, msg any) ([]byte, error)` | JSON 编码为帧 |
| `EncodeRaw(msgType byte, payload []byte) []byte` | 原始负载编码为帧 |
| `DecodeHeader(r io.Reader) (msgType byte, payloadLen uint32, err error)` | 解码帧头 |
| `DecodePayload(r io.Reader, payloadLen uint32) ([]byte, error)` | 读取完整负载 |
| `DecodeMessage(data []byte) (msgType byte, payload []byte, err error)` | 一步解码完整帧 |
| `UnmarshalPayload(data []byte, v any) error` | JSON 反序列化负载 |
| `NewFrameReader(r io.Reader) *FrameReader` | 创建流式帧读取器 |
| `BEBytes(v uint32) [4]byte` | uint32 转大端序字节 |
| `ReadBE(r io.Reader) (uint32, error)` | 读取 4 字节 BE uint32 |

## 测试

**140 个测试用例，5 个包，全部通过。**

```bash
go test ./...            # 全量测试（约 12 秒）
go test -v ./...         # 详细输出
go test -bench=. ./protocol/  # 基准测试
```

### 测试覆盖

| 包 | 测试数 | 类型 |
|----|--------|------|
| protocol | 69 | 消息往返、帧解析、并发编解码、边界条件、大负载 |
| discovery | 29 | 广播地址、IP 枚举、缓冲区读取器、Broadcaster/Listener 生命周期 |
| transport | 33 | 引擎启动/停止、连接池、RTT EMA、并发安全性、回调完整性 |
| storage | 19 | 保存/读取/哈希/缺失检测、并发读写、1000 分块压力 |
| 根包（集成+场景） | 18 | 完整文件分发、Seeder 协助、大文件流、断连恢复、节点动态加入 |

## 性能

在本地回环（localhost）基准测试：

| 操作 | 吞吐 |
|------|------|
| 帧编码 (1KB JSON) | ~870 ns/op |
| 帧解码 (1KB JSON) | ~690 ns/op |
| 帧往返 (编码+解码) | ~1.5 μs/op |
| TCP 分块传输 (4KB) | ~500 μs/op |
| UDP 心跳广播 | 2s/次（可配置） |

## 相关项目

- [BitStorm](https://github.com/cuicuisha233/BitStorm) — 基于本引擎的完整桌面 GUI 应用（Wails + React）

## 许可证

MIT — 详见 [LICENSE](LICENSE)
