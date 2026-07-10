package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// 测试辅助函数
// ============================================================================

// assertFrameRoundtrip 验证消息的完整编解码往返过程。
func assertFrameRoundtrip(t *testing.T, msgType byte, msg interface{}, expectedType byte) {
	t.Helper()

	// 步骤1: 编码为 BSFD 帧
	frame, err := Encode(msgType, msg)
	if err != nil {
		t.Fatalf("编码失败 (type=0x%02X): %v", msgType, err)
	}

	// 步骤2: 解码帧头
	reader := bytes.NewReader(frame)
	decodedType, payloadLen, err := DecodeHeader(reader)
	if err != nil {
		t.Fatalf("解码帧头失败 (type=0x%02X): %v", msgType, err)
	}

	if decodedType != expectedType {
		t.Errorf("消息类型不匹配: 期望 0x%02X, 实际 0x%02X", expectedType, decodedType)
	}

	// 步骤3: 解码负载
	payload, err := DecodePayload(reader, payloadLen)
	if err != nil {
		t.Fatalf("解码负载失败 (type=0x%02X): %v", msgType, err)
	}

	if len(payload) != int(payloadLen) {
		t.Errorf("负载长度不匹配: 期望 %d, 实际 %d", payloadLen, len(payload))
	}

	// 步骤4: 反序列化并验证 JSON 一致性
	var decoded map[string]interface{}
	if err := UnmarshalPayload(payload, &decoded); err != nil {
		t.Fatalf("反序列化负载失败 (type=0x%02X): %v", msgType, err)
	}

	// 重新序列化原始消息以进行 JSON 一致性比较
	originalJSON, _ := json.Marshal(msg)
	var originalMap map[string]interface{}
	json.Unmarshal(originalJSON, &originalMap)

	for k, v := range originalMap {
		decodedVal, ok := decoded[k]
		if !ok {
			t.Errorf("反序列化后缺少字段 %q (type=0x%02X)", k, msgType)
			continue
		}
		// 对于 base64 编码的 []byte 字段，只比较存在性
		if _, isStr := v.(string); isStr {
			if _, isStr2 := decodedVal.(string); !isStr2 {
				t.Errorf("字段 %q 类型不匹配 (type=0x%02X)", k, msgType)
			}
		}
	}
}

// ============================================================================
// 帧编码/解码基础测试
// ============================================================================

func TestFrameEncodeDecode_Roundtrip(t *testing.T) {
	// 测试所有消息类型的编解码往返
	testCases := []struct {
		name    string
		msgType byte
		msg     interface{}
	}{
		{"Heartbeat", TypeHeartbeat, Heartbeat{
			PeerID: "peer-001", Name: "测试节点", IP: "192.168.1.100",
			TCPPort: 26932, Role: "client", TotalChunks: 100,
		}},
		{"Hello", TypeHello, Hello{PeerID: "client-host-12345"}},
		{"FileInfo", TypeFileInfo, FileInfo{
			FileHash: "abc123def456", FileName: "test.zip", FileSize: 1048576,
			ChunkSize: 1048576, TotalChunks: 1, ChunkHashes: []string{"hash1"},
		}},
		{"StartDist", TypeStartDist, StartDist{FileHash: "abc123", SavePath: "C:\\Downloads"}},
		{"ChunkRequest", TypeChunkRequest, ChunkRequest{FileHash: "filehash", ChunkIndex: 42}},
		{"BlockRequest", TypeBlockRequest, BlockRequest{
			FileHash: "filehash", ChunkIndex: 5, BlockOffset: 65536, BlockSize: 16384,
		}},
		{"ChunkData", TypeChunkData, ChunkData{
			FileHash: "filehash", ChunkIndex: 0, Data: []byte("chunk data content"),
		}},
		{"Cancel", TypeCancel, Cancel{FileHash: "abc123"}},
		{"Finish", TypeFinish, Finish{FileHash: "abc123"}},
		{"KeepAlive", TypeKeepAlive, KeepAlive{PeerID: "peer-001"}},
		{"Disconnect", TypeDisconnect, Disconnect{PeerID: "peer-001"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assertFrameRoundtrip(t, tc.msgType, tc.msg, tc.msgType)
		})
	}
}

func TestFrameEncode_DifferentPayloadSizes(t *testing.T) {
	sizes := []int{0, 1, 64, 256, 1024, 65536}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("payload_%d_bytes", size), func(t *testing.T) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}
			msg := ChunkData{
				FileHash:   "test",
				ChunkIndex: 0,
				Data:       data,
			}

			frame, err := Encode(TypeChunkData, msg)
			if err != nil {
				t.Fatalf("编码 %d 字节负载失败: %v", size, err)
			}

			reader := bytes.NewReader(frame)
			msgType, payloadLen, err := DecodeHeader(reader)
			if err != nil {
				t.Fatalf("解码 %d 字节帧头失败: %v", size, err)
			}
			if msgType != TypeChunkData {
				t.Errorf("消息类型: 期望 0x%02X, 实际 0x%02X", TypeChunkData, msgType)
			}

			payload, err := DecodePayload(reader, payloadLen)
			if err != nil {
				t.Fatalf("解码 %d 字节负载失败: %v", size, err)
			}

			var decoded ChunkData
			if err := UnmarshalPayload(payload, &decoded); err != nil {
				t.Fatalf("反序列化 %d 字节负载失败: %v", size, err)
			}

			if len(decoded.Data) != size {
				t.Errorf("数据长度: 期望 %d, 实际 %d", size, len(decoded.Data))
			}
		})
	}
}

// ============================================================================
// FrameReader 测试
// ============================================================================

func TestFrameReader_SingleFrame(t *testing.T) {
	msg := Hello{PeerID: "test-peer-42"}
	frame, _ := Encode(TypeHello, msg)
	reader := bytes.NewReader(frame)
	fr := NewFrameReader(reader)

	msgType, payload, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame 失败: %v", err)
	}

	if msgType != TypeHello {
		t.Errorf("消息类型: 期望 0x%02X, 实际 0x%02X", TypeHello, msgType)
	}

	var decoded Hello
	if err := UnmarshalPayload(payload, &decoded); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if decoded.PeerID != "test-peer-42" {
		t.Errorf("PeerID: 期望 test-peer-42, 实际 %s", decoded.PeerID)
	}
}

func TestFrameReader_MultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}

	for _, m := range messages {
		msg := Hello{PeerID: m}
		frame, _ := Encode(TypeHello, msg)
		buf.Write(frame)
	}

	fr := NewFrameReader(&buf)
	for i, expectedPeerID := range messages {
		msgType, payload, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame #%d 失败: %v", i, err)
		}
		if msgType != TypeHello {
			t.Errorf("ReadFrame #%d: 类型 0x%02X, 期望 0x%02X", i, msgType, TypeHello)
		}
		var decoded Hello
		UnmarshalPayload(payload, &decoded)
		if decoded.PeerID != expectedPeerID {
			t.Errorf("ReadFrame #%d: PeerID %q, 期望 %q", i, decoded.PeerID, expectedPeerID)
		}
	}
}

func TestFrameReader_EmptyReader(t *testing.T) {
	// 空 Reader 应立即返回错误
	reader := bytes.NewReader([]byte{})
	fr := NewFrameReader(reader)
	_, _, err := fr.ReadFrame()
	if err == nil {
		t.Error("空 Reader 应返回错误")
	}
}

func TestFrameReader_PartialHeader(t *testing.T) {
	// 只提供部分帧头（少于 9 字节）
	// 构造一个完整帧用来提供足够的底层数据，但限制 reader 只能读到前 i 个字节
	fullMsg := Hello{PeerID: "test"}
	fullFrame, _ := Encode(TypeHello, fullMsg)

	for i := 1; i < HeaderLen; i++ {
		t.Run(fmt.Sprintf("%d_bytes", i), func(t *testing.T) {
			reader := bytes.NewReader(fullFrame[:i])
			fr := NewFrameReader(reader)
			_, _, err := fr.ReadFrame()
			if err == nil {
				t.Errorf("%d 字节应触发错误（帧头需要 %d 字节）", i, HeaderLen)
			}
		})
	}
}

func TestFrameReader_PartialPayload(t *testing.T) {
	// 帧头完整但负载不完整
	header := make([]byte, HeaderLen)
	copy(header[0:4], MagicBytes)
	header[MagicLen] = TypeHello
	// 声明 100 字节负载，但只给 50 字节
	payloadLen := uint32(100)
	header[MagicLen+1] = byte(payloadLen >> 24)
	header[MagicLen+2] = byte(payloadLen >> 16)
	header[MagicLen+3] = byte(payloadLen >> 8)
	header[MagicLen+4] = byte(payloadLen)

	partialPayload := make([]byte, 50)
	buf := append(header, partialPayload...)

	reader := bytes.NewReader(buf)
	fr := NewFrameReader(reader)
	_, _, err := fr.ReadFrame()
	if err == nil {
		t.Error("不完整的负载应触发读取错误")
	}
}

// ============================================================================
// 错误处理测试
// ============================================================================

func TestDecodeHeader_InvalidMagic(t *testing.T) {
	invalidMagics := []string{"BSFX", "XXXX", "BSF", "bsfd", "0000", "\x00\x00\x00\x00"}

	for _, magic := range invalidMagics {
		t.Run(fmt.Sprintf("magic_%q", magic), func(t *testing.T) {
			buf := make([]byte, HeaderLen)
			copy(buf[0:4], magic)
			reader := bytes.NewReader(buf)
			_, _, err := DecodeHeader(reader)
			if err != ErrInvalidMagic {
				t.Errorf("期望 ErrInvalidMagic, 实际: %v", err)
			}
		})
	}
}

func TestDecodeHeader_ShortHeader(t *testing.T) {
	for i := 0; i < HeaderLen; i++ {
		t.Run(fmt.Sprintf("%d_bytes", i), func(t *testing.T) {
			frame := make([]byte, i)
			reader := bytes.NewReader(frame)
			_, _, err := DecodeHeader(reader)
			if err == nil {
				t.Errorf("%d 字节应触发错误（帧头需要 %d 字节）", i, HeaderLen)
			}
		})
	}
}

func TestDecodeHeader_PayloadTooBig(t *testing.T) {
	// 构造一个声明超出最大负载的帧头
	buf := make([]byte, HeaderLen)
	copy(buf[0:4], MagicBytes)
	buf[MagicLen] = TypeHeartbeat
	// 设置超大 payloadLen (MaxPayload + 1)
	huge := uint32(MaxPayload) + 1
	buf[MagicLen+1] = byte(huge >> 24)
	buf[MagicLen+2] = byte(huge >> 16)
	buf[MagicLen+3] = byte(huge >> 8)
	buf[MagicLen+4] = byte(huge)

	reader := bytes.NewReader(buf)
	_, _, err := DecodeHeader(reader)
	if err != ErrPayloadTooBig {
		t.Errorf("超大负载应触发 ErrPayloadTooBig, 实际: %v", err)
	}
}

func TestDecodeHeader_ExactMaxPayload(t *testing.T) {
	// 恰好 MaxPayload 大小应被接受
	buf := make([]byte, HeaderLen)
	copy(buf[0:4], MagicBytes)
	buf[MagicLen] = TypeHeartbeat
	exactMax := uint32(MaxPayload)
	buf[MagicLen+1] = byte(exactMax >> 24)
	buf[MagicLen+2] = byte(exactMax >> 16)
	buf[MagicLen+3] = byte(exactMax >> 8)
	buf[MagicLen+4] = byte(exactMax)

	reader := bytes.NewReader(buf)
	msgType, payloadLen, err := DecodeHeader(reader)
	if err != nil {
		t.Fatalf("恰好最大负载的帧头应被接受: %v", err)
	}
	if msgType != TypeHeartbeat {
		t.Errorf("类型: 期望 0x%02X, 实际 0x%02X", TypeHeartbeat, msgType)
	}
	if payloadLen != exactMax {
		t.Errorf("负载长度: 期望 %d, 实际 %d", exactMax, payloadLen)
	}
}

// ============================================================================
// EncodeRaw 测试
// ============================================================================

func TestEncodeRaw_Basic(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	frame := EncodeRaw(TypeChunkDataBin, payload)
	if frame == nil {
		t.Fatal("EncodeRaw 返回 nil")
	}

	expectedLen := HeaderLen + len(payload)
	if len(frame) != expectedLen {
		t.Errorf("帧长度: 期望 %d, 实际 %d", expectedLen, len(frame))
	}

	// 验证 magic
	if string(frame[0:4]) != MagicBytes {
		t.Errorf("Magic: 期望 %q, 实际 %q", MagicBytes, string(frame[0:4]))
	}

	// 验证类型
	if frame[MagicLen] != TypeChunkDataBin {
		t.Errorf("类型: 期望 0x%02X, 实际 0x%02X", TypeChunkDataBin, frame[MagicLen])
	}

	// 验证负载
	for i, b := range payload {
		if frame[HeaderLen+i] != b {
			t.Errorf("负载[%d]: 期望 0x%02X, 实际 0x%02X", i, b, frame[HeaderLen+i])
		}
	}
}

func TestEncodeRaw_PayloadTooBig(t *testing.T) {
	huge := make([]byte, MaxPayload+1)
	frame := EncodeRaw(TypeChunkDataBin, huge)
	if frame != nil {
		t.Error("超大负载应返回 nil")
	}
}

func TestEncodeRaw_EmptyPayload(t *testing.T) {
	frame := EncodeRaw(TypeKeepAlive, []byte{})
	if frame == nil {
		t.Fatal("空负载应返回有效帧")
	}
	if len(frame) != HeaderLen {
		t.Errorf("空负载帧长度: 期望 %d, 实际 %d", HeaderLen, len(frame))
	}

	// 验证 payloadLen 为 0
	reader := bytes.NewReader(frame)
	_, payloadLen, err := DecodeHeader(reader)
	if err != nil {
		t.Fatalf("解码空负载帧失败: %v", err)
	}
	if payloadLen != 0 {
		t.Errorf("负载长度: 期望 0, 实际 %d", payloadLen)
	}
}

// ============================================================================
// 消息序列化测试
// ============================================================================

func TestHeartbeat_JSONRoundtrip(t *testing.T) {
	hb := Heartbeat{
		PeerID:          "peer-001",
		Name:            "测试节点",
		IP:              "10.0.0.1",
		TCPPort:         26932,
		Role:            "client",
		ChunkBitmap:     []byte{0xFF, 0x0F},
		TotalChunks:     12,
		PreloadedHashes: []string{"hashA", "hashB"},
	}

	data, err := json.Marshal(hb)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	var decoded Heartbeat
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}

	if decoded.PeerID != hb.PeerID {
		t.Errorf("PeerID: 期望 %q, 实际 %q", hb.PeerID, decoded.PeerID)
	}
	if decoded.Name != hb.Name {
		t.Errorf("Name: 期望 %q, 实际 %q", hb.Name, decoded.Name)
	}
	if decoded.TCPPort != hb.TCPPort {
		t.Errorf("TCPPort: 期望 %d, 实际 %d", hb.TCPPort, decoded.TCPPort)
	}
	if decoded.Role != hb.Role {
		t.Errorf("Role: 期望 %q, 实际 %q", hb.Role, decoded.Role)
	}
	if decoded.TotalChunks != hb.TotalChunks {
		t.Errorf("TotalChunks: 期望 %d, 实际 %d", hb.TotalChunks, decoded.TotalChunks)
	}
	if len(decoded.ChunkBitmap) != len(hb.ChunkBitmap) {
		t.Errorf("ChunkBitmap 长度: 期望 %d, 实际 %d", len(hb.ChunkBitmap), len(decoded.ChunkBitmap))
	}
	if len(decoded.PreloadedHashes) != len(hb.PreloadedHashes) {
		t.Errorf("PreloadedHashes 长度: 期望 %d, 实际 %d", len(hb.PreloadedHashes), len(decoded.PreloadedHashes))
	}
}

func TestSeedFile_JSONRoundtrip(t *testing.T) {
	seed := SeedFile{
		Version:     1,
		Type:        "file",
		FileName:    "project.zip",
		FileSize:    1024 * 1024 * 100,
		FileHash:    "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
		ChunkSize:   1 << 20,
		TotalChunks: 100,
		ChunkHashes: []string{"c1", "c2", "c3"},
	}

	data, _ := json.Marshal(seed)
	var decoded SeedFile
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("反序列化 SeedFile 失败: %v", err)
	}

	if decoded.Version != seed.Version {
		t.Errorf("Version: 期望 %d, 实际 %d", seed.Version, decoded.Version)
	}
	if decoded.FileHash != seed.FileHash {
		t.Errorf("FileHash 不匹配")
	}
	if decoded.TotalChunks != seed.TotalChunks {
		t.Errorf("TotalChunks: 期望 %d, 实际 %d", seed.TotalChunks, decoded.TotalChunks)
	}
}

func TestPeerInfo_JSONRoundtrip(t *testing.T) {
	pi := PeerInfo{
		ID:              "peer-abc",
		IP:              "192.168.1.50",
		Port:            26932,
		Hostname:        "DESKTOP-XYZ",
		IsAdmin:         true,
		IsSeeder:        true,
		ChunkBitmap:     []byte{0xAA, 0xBB},
		PreloadedHashes: []string{"h1"},
	}

	data, _ := json.Marshal(pi)
	var decoded PeerInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("反序列化 PeerInfo 失败: %v", err)
	}

	if decoded.ID != pi.ID {
		t.Errorf("ID 不匹配")
	}
	if !decoded.IsAdmin {
		t.Error("IsAdmin 应为 true")
	}
	if !decoded.IsSeeder {
		t.Error("IsSeeder 应为 true")
	}
}

func TestUnmarshalPayload_InvalidJSON(t *testing.T) {
	invalidJSON := []byte("{invalid json}")
	var result Hello
	err := UnmarshalPayload(invalidJSON, &result)
	if err == nil {
		t.Error("无效 JSON 应返回错误")
	}
}

func TestUnmarshalPayload_EmptyPayload(t *testing.T) {
	var result Hello
	err := UnmarshalPayload([]byte{}, &result)
	if err == nil {
		t.Error("空负载应返回错误")
	}
}

// ============================================================================
// 常量验证测试
// ============================================================================

func TestConstants_MessageTypeUniqueness(t *testing.T) {
	types := map[byte]string{
		TypeHeartbeat:    "TypeHeartbeat",
		TypeHello:        "TypeHello",
		TypeFileInfo:     "TypeFileInfo",
		TypeStartDist:    "TypeStartDist",
		TypeChunkRequest: "TypeChunkRequest",
		TypeChunkData:    "TypeChunkData",
		TypeChunkDataBin: "TypeChunkDataBin",
		TypeBlockRequest: "TypeBlockRequest",
		TypeBlockDataBin: "TypeBlockDataBin",
		TypeCancel:       "TypeCancel",
		TypeFinish:       "TypeFinish",
		TypeKeepAlive:    "TypeKeepAlive",
		TypeDisconnect:   "TypeDisconnect",
	}

	seen := make(map[byte]bool)
	for typ, name := range types {
		if seen[typ] {
			t.Errorf("消息类型 0x%02X (%s) 重复定义", typ, name)
		}
		seen[typ] = true

		// 检查值范围
		if typ == 0x00 {
			t.Errorf("%s 值不能为 0", name)
		}
	}

	if len(types) != 13 {
		t.Errorf("消息类型总数: 期望 13, 实际 %d", len(types))
	}
}

func TestConstants_FrameConstants(t *testing.T) {
	if MagicBytes != "BSFD" {
		t.Errorf("MagicBytes: 期望 BSFD, 实际 %s", MagicBytes)
	}
	if HeaderLen != 9 {
		t.Errorf("HeaderLen: 期望 9, 实际 %d", HeaderLen)
	}
	if MaxPayload != 32<<20 {
		t.Errorf("MaxPayload: 期望 %d (32MiB), 实际 %d", 32<<20, MaxPayload)
	}
	if Version != 1 {
		t.Errorf("Version: 期望 1, 实际 %d", Version)
	}
}

// ============================================================================
// 帧格式一致性测试
// ============================================================================

func TestFrameFormat_HeaderStructure(t *testing.T) {
	msg := Hello{PeerID: "test"}
	frame, _ := Encode(TypeHello, msg)

	if len(frame) < HeaderLen {
		t.Fatal("帧长度小于帧头长度")
	}

	// 验证帧头结构: [4B Magic][1B Type][4B PayloadLen BE]
	if string(frame[0:4]) != MagicBytes {
		t.Errorf("Magic 位置 [0:4]: 期望 %q, 实际 %q", MagicBytes, string(frame[0:4]))
	}
	if frame[4] != TypeHello {
		t.Errorf("Type 位置 [4]: 期望 0x%02X, 实际 0x%02X", TypeHello, frame[4])
	}

	// PayloadLen (BE) 在 [5:9]
	payloadLen := uint32(frame[5])<<24 | uint32(frame[6])<<16 | uint32(frame[7])<<8 | uint32(frame[8])
	expectedPayloadLen := len(frame) - HeaderLen
	if int(payloadLen) != expectedPayloadLen {
		t.Errorf("PayloadLen: 期望 %d, 实际 %d", expectedPayloadLen, payloadLen)
	}
}

// ============================================================================
// 并发安全测试
// ============================================================================

func TestEncode_Concurrent(t *testing.T) {
	const goroutines = 50
	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			msg := Hello{PeerID: fmt.Sprintf("peer-%d", id)}
			frame, err := Encode(TypeHello, msg)
			if err != nil {
				t.Errorf("并发编码 #%d 失败: %v", id, err)
			}
			if frame == nil {
				t.Errorf("并发编码 #%d 返回 nil", id)
			}
			done <- true
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func TestFrameReader_Concurrent(t *testing.T) {
	// 多个 FrameReader 各自独立读取
	const goroutines = 10
	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			msg := Hello{PeerID: fmt.Sprintf("peer-%d", id)}
			frame, _ := Encode(TypeHello, msg)
			reader := bytes.NewReader(frame)
			fr := NewFrameReader(reader)
			_, _, err := fr.ReadFrame()
			if err != nil {
				t.Errorf("并发 FrameReader #%d 失败: %v", id, err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

// ============================================================================
// 边界条件测试
// ============================================================================

func TestEncode_NilMessage(t *testing.T) {
	// nil interface{} → json.Marshal(nil) → "null"
	frame, err := Encode(TypeKeepAlive, nil)
	if err != nil {
		t.Fatalf("编码 nil 失败: %v", err)
	}
	if frame == nil {
		t.Fatal("nil 消息应生成有效帧（JSON null）")
	}
}

func TestDecodeMessage_Roundtrip(t *testing.T) {
	msg := Hello{PeerID: "decode-message-test"}
	frame, _ := Encode(TypeHello, msg)

	reader := bytes.NewReader(frame)
	decodedType, payload, err := DecodeMessage(reader)
	if err != nil {
		t.Fatalf("DecodeMessage 失败: %v", err)
	}
	if decodedType != TypeHello {
		t.Errorf("类型: 期望 0x%02X, 实际 0x%02X", TypeHello, decodedType)
	}

	var decoded Hello
	UnmarshalPayload(payload, &decoded)
	if decoded.PeerID != msg.PeerID {
		t.Errorf("PeerID: 期望 %q, 实际 %q", msg.PeerID, decoded.PeerID)
	}
}

func TestEncodeMessage_Alias(t *testing.T) {
	msg := Hello{PeerID: "alias-test"}
	f1, _ := Encode(TypeHello, msg)
	f2, _ := EncodeMessage(TypeHello, msg)

	if len(f1) != len(f2) {
		t.Errorf("Encode 和 EncodeMessage 产生不同长度的帧: %d vs %d", len(f1), len(f2))
	}
	for i := range f1 {
		if f1[i] != f2[i] {
			t.Errorf("Encode 和 EncodeMessage 在字节 %d 不一致", i)
			break
		}
	}
}

// ============================================================================
// 大负载测试
// ============================================================================

func TestLargePayload_NearMax(t *testing.T) {
	// 测试接近最大负载 (1 MB — MaxPayload 是 32 MB, 太大会很慢)
	const testSize = 1 << 20 // 1 MB
	data := make([]byte, testSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	msg := ChunkData{FileHash: "large", ChunkIndex: 0, Data: data}

	frame, err := Encode(TypeChunkData, msg)
	if err != nil {
		t.Fatalf("编码 1MB 负载失败: %v", err)
	}

	// 解码并验证
	reader := bytes.NewReader(frame)
	fr := NewFrameReader(reader)
	_, payload, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("解码 1MB 负载失败: %v", err)
	}

	var decoded ChunkData
	if err := UnmarshalPayload(payload, &decoded); err != nil {
		t.Fatalf("反序列化 1MB 负载失败: %v", err)
	}

	if len(decoded.Data) != testSize {
		t.Errorf("数据长度: 期望 %d, 实际 %d", testSize, len(decoded.Data))
	}

	// 抽样验证几个位置
	for _, idx := range []int{0, testSize / 2, testSize - 1} {
		if decoded.Data[idx] != byte(idx%256) {
			t.Errorf("数据[%d]: 期望 %d, 实际 %d", idx, byte(idx%256), decoded.Data[idx])
		}
	}
}

// ============================================================================
// BEBytes / ReadBE / Buffer 辅助函数测试
// ============================================================================

func TestBEBytes(t *testing.T) {
	cases := []struct {
		input    uint32
		expected [4]byte
	}{
		{0x00000000, [4]byte{0, 0, 0, 0}},
		{0x00000001, [4]byte{0, 0, 0, 1}},
		{0x00000100, [4]byte{0, 0, 1, 0}},
		{0x01000000, [4]byte{1, 0, 0, 0}},
		{0xDEADBEEF, [4]byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{0xFFFFFFFF, [4]byte{0xFF, 0xFF, 0xFF, 0xFF}},
	}

	for _, c := range cases {
		result := BEBytes(c.input)
		if result != c.expected {
			t.Errorf("BEBytes(0x%08X): 期望 %v, 实际 %v", c.input, c.expected, result)
		}
	}
}

func TestReadBE(t *testing.T) {
	cases := []struct {
		input    []byte
		expected uint32
	}{
		{[]byte{0, 0, 0, 0}, 0},
		{[]byte{0, 0, 0, 1}, 1},
		{[]byte{0xDE, 0xAD, 0xBE, 0xEF}, 0xDEADBEEF},
		{[]byte{0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFF},
	}

	for _, c := range cases {
		result := ReadBE(c.input)
		if result != c.expected {
			t.Errorf("ReadBE(%v): 期望 0x%08X, 实际 0x%08X", c.input, c.expected, result)
		}
	}
}

func TestBuffer_WriteBE(t *testing.T) {
	buf := NewBuffer()
	buf.WriteBE(0xDEADBEEF)
	buf.WriteBE(0xCAFEBABE)

	result := buf.Bytes()
	expected := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}

	if len(result) != len(expected) {
		t.Fatalf("Buffer 长度: 期望 %d, 实际 %d", len(expected), len(result))
	}
	for i, b := range expected {
		if result[i] != b {
			t.Errorf("Buffer[%d]: 期望 0x%02X, 实际 0x%02X", i, b, result[i])
		}
	}
}

func TestJoinBytes(t *testing.T) {
	a := []byte{1, 2, 3}
	b := []byte{4, 5, 6}
	result := JoinBytes(a, b)

	if len(result) != 6 {
		t.Errorf("长度: 期望 6, 实际 %d", len(result))
	}
	for i, expected := range []byte{1, 2, 3, 4, 5, 6} {
		if result[i] != expected {
			t.Errorf("[%d]: 期望 %d, 实际 %d", i, expected, result[i])
		}
	}
}

func TestReadPayload(t *testing.T) {
	data := []byte("hello world")
	reader := bytes.NewReader(data)
	result, err := ReadPayload(reader, len(data))
	if err != nil {
		t.Fatalf("ReadPayload 失败: %v", err)
	}
	if string(result) != string(data) {
		t.Errorf("内容: 期望 %q, 实际 %q", data, result)
	}

	// 读取超过可用的字节应失败
	_, err = ReadPayload(reader, 100)
	if err == nil {
		t.Error("ReadPayload 超过可用字节应失败")
	}
}

// ============================================================================
// 集成往返测试: 模拟完整 P2P 消息交换
// ============================================================================

func TestIntegration_FullMessageExchange(t *testing.T) {
	// 模拟一个完整的 P2P 消息交换流程:
	// 1. Client → Admin: Hello
	// 2. Admin → Client: FileInfo
	// 3. Admin → Client: StartDist
	// 4. Client → Admin: ChunkRequest
	// 5. Admin → Client: ChunkDataBin (binary)
	// 6. Admin → Client: Finish

	var buf bytes.Buffer

	// 1. Hello
	hello := Hello{PeerID: "client-01"}
	frame, _ := Encode(TypeHello, hello)
	buf.Write(frame)

	// 2. FileInfo
	fi := FileInfo{
		FileHash: "abc123", FileName: "data.zip", FileSize: 1000000,
		ChunkSize: 1 << 20, TotalChunks: 1, ChunkHashes: []string{"h1"},
	}
	frame, _ = Encode(TypeFileInfo, fi)
	buf.Write(frame)

	// 3. StartDist
	sd := StartDist{FileHash: "abc123", SavePath: "/downloads"}
	frame, _ = Encode(TypeStartDist, sd)
	buf.Write(frame)

	// 4. ChunkRequest
	cr := ChunkRequest{FileHash: "abc123", ChunkIndex: 0}
	frame, _ = Encode(TypeChunkRequest, cr)
	buf.Write(frame)

	// 5. ChunkDataBin (binary)
	payload := make([]byte, 8+100) // 4B index + 4B offset + data
	copy(payload[4:8], []byte{0, 0, 0, 0})
	frame = EncodeRaw(TypeChunkDataBin, payload)
	buf.Write(frame)

	// 6. Finish
	fin := Finish{FileHash: "abc123"}
	frame, _ = Encode(TypeFinish, fin)
	buf.Write(frame)

	// 读取所有帧
	fr := NewFrameReader(&buf)
	expectedTypes := []byte{TypeHello, TypeFileInfo, TypeStartDist, TypeChunkRequest, TypeChunkDataBin, TypeFinish}
	for i, expected := range expectedTypes {
		msgType, payload, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("帧 #%d (type=0x%02X) 读取失败: %v", i, expected, err)
		}
		if msgType != expected {
			t.Errorf("帧 #%d: 类型 0x%02X, 期望 0x%02X", i, msgType, expected)
		}

		// 非二进制帧验证 JSON 可解析
		if msgType != TypeChunkDataBin {
			var m map[string]interface{}
			if err := UnmarshalPayload(payload, &m); err != nil {
				t.Errorf("帧 #%d (type=0x%02X) JSON 解析失败: %v", i, msgType, err)
			}
		}
	}

	// 不应再有额外帧
	_, _, err := fr.ReadFrame()
	if err == nil {
		t.Error("读取完所有帧后应返回错误（EOF）")
	}
}

// ============================================================================
// 性能基准测试
// ============================================================================

func BenchmarkEncode_Hello(b *testing.B) {
	msg := Hello{PeerID: "bench-peer-12345"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(TypeHello, msg)
	}
}

func BenchmarkDecode_Hello(b *testing.B) {
	msg := Hello{PeerID: "bench-peer-12345"}
	frame, _ := Encode(TypeHello, msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(frame)
		DecodeMessage(reader)
	}
}

func BenchmarkEncode_FileInfo(b *testing.B) {
	fi := FileInfo{
		FileHash: "abc123def456", FileName: "test.zip", FileSize: 1048576,
		ChunkSize: 1048576, TotalChunks: 100,
		ChunkHashes: func() []string {
			h := make([]string, 100)
			for i := range h {
				h[i] = strings.Repeat("a", 64)
			}
			return h
		}(),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(TypeFileInfo, fi)
	}
}

func BenchmarkEncodeRaw_1MB(b *testing.B) {
	data := make([]byte, 1<<20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeRaw(TypeChunkDataBin, data)
	}
}

func BenchmarkFrameReader_1MB(b *testing.B) {
	data := make([]byte, 1<<20)
	frame := EncodeRaw(TypeChunkDataBin, data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(frame)
		fr := NewFrameReader(reader)
		fr.ReadFrame()
	}
}

// ============================================================================
// 防止编译器优化基准测试结果
// ============================================================================

var (
	sinkBytes  []byte
	sinkByte   byte
	sinkError  error
	sinkString string
	sinkTime   time.Duration
	_          = sinkBytes
	_          = sinkByte
	_          = sinkError
	_          = sinkString
	_          = sinkTime
	_          = io.EOF
)
