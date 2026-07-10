# BSFD — BitStorm 文件分发协议引擎

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/测试-69个全部通过-brightgreen)](#)

轻量级、可嵌入的 P2P 文件分发协议引擎，专为高吞吐局域网环境设计。**零外部依赖**，仅使用 Go 标准库。

## 功能特性

- **UDP 自动发现** — 节点通过周期性心跳广播自身，客户端自动发现管理端
- **TCP 多路复用** — 每节点 4 条连接，基于哈希的负载分配
- **块级流水线传输** — 16KB 块传输，动态并发窗口
- **RTT 感知流控** — EMA 平滑往返时延跟踪，自适应并发
- **种子文件 (.bsfd)** — 可移植的元数据格式，支持预部署校验
- **可嵌入设计** — 纯回调驱动 API，不依赖任何 GUI 框架或文件系统

## 架构

```
bsfd/
├── protocol/  — 帧格式 (BSFD 魔数、类型、负载), 13 种消息类型
├── discovery/ — UDP 心跳广播、节点注册表、网络工具函数
├── transport/ — TCP 连接池、RTT EMA、块调度器
└── storage/   — 分块存储接口 (由宿主应用实现)
```

### 线缆格式 (v1)

```
[4B 魔数 "BSFD"][1B 类型][4B 负载长度 大端序][N 字节负载]
```

| 字段   | 大小  | 说明                   |
|--------|-------|------------------------|
| 魔数   | 4 字节 | 固定为 `BSFD`          |
| 类型   | 1 字节 | 消息类型 (见下表)       |
| 负载长度 | 4 字节 | 大端序 uint32          |
| 负载   | N 字节 | JSON 或二进制数据       |

### 消息类型

| 类型 | 名称         | 方向         | 格式   |
|------|-------------|--------------|--------|
| 0x01 | 心跳        | 双向         | JSON   |
| 0x02 | 握手        | 客户端→管理端 | JSON   |
| 0x03 | 文件信息    | 管理端→客户端 | JSON   |
| 0x04 | 开始分发    | 管理端→客户端 | JSON   |
| 0x10 | 请求分块    | 节点互发     | JSON   |
| 0x11 | JSON分块    | 节点互发     | JSON   |
| 0x12 | 二进制分块  | 节点互发     | 二进制 |
| 0x13 | 请求块      | 节点互发     | JSON   |
| 0x14 | 二进制块    | 节点互发     | 二进制 |
| 0x30 | 取消分发    | 管理端→客户端 | JSON   |
| 0x31 | 分发完成    | 管理端→客户端 | JSON   |
| 0xFE | 保活        | 双向         | JSON   |
| 0xFF | 断开        | 双向         | JSON   |

## 安装

```bash
go get github.com/cuicuisha233/bsfd
```

## 快速开始

```go
package main

import (
    "log"

    "github.com/cuicuisha233/bsfd/discovery"
    "github.com/cuicuisha233/bsfd/protocol"
    "github.com/cuicuisha233/bsfd/transport"
)

func main() {
    // === 发现层 ===
    discCfg := discovery.DefaultConfig()
    listener := discovery.NewListener(discCfg)
    listener.OnDiscover(func(admin discovery.DiscoveredAdmin) {
        log.Printf("发现管理端: %s 地址 %s:%d", admin.Name, admin.IP, admin.TCPPort)
    })
    listener.Start()
    defer listener.Stop()

    broadcaster := discovery.NewBroadcaster(
        discCfg,
        "my-peer-id",
        discovery.Hostname(),
        discovery.LocalIPs()[0],
        26932,
        "client",
        0,
        nil, nil,
    )
    broadcaster.Start()
    defer broadcaster.Stop()

    // === 传输层 ===
    tCfg := transport.DefaultConfig()
    engine := transport.New(tCfg)
    engine.SetLocalPeerID("my-peer-id")
    engine.SetCallbacks(transport.Callbacks{
        OnPeerConnected: func(peerID string) {
            log.Printf("节点已连接: %s", peerID)
        },
        OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
            log.Printf("收到分块 %d 来自 %s (%d 字节)", chunkIndex, peerID, len(data))
        },
    })
    engine.Start()
    defer engine.Stop()

    // === 协议层 ===
    frame, _ := protocol.Encode(protocol.TypeHello, protocol.Hello{PeerID: "my-peer-id"})
    _ = frame
}
```

## 测试

```bash
# 运行全部测试
go test ./...

# 详细输出
go test -v ./...

# 基准测试
go test -bench=. ./protocol/
```

## 相关项目

- [BitStorm](https://github.com/cuicuisha233/BitStorm) — 基于本引擎的完整桌面端应用

## 许可证

MIT — 详见 [LICENSE](LICENSE)
