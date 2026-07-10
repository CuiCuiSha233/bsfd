// Package discovery provides UDP-based automatic peer discovery for BSFD.
//
// Every node broadcasts a periodic Heartbeat on UDP, and listens for heartbeats
// from other nodes. Admin nodes are discovered automatically by clients;
// client nodes register in the admin's peer registry.
package discovery

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// Config holds discovery parameters.
type Config struct {
	UDPPort           int
	BroadcastInterval time.Duration
	ReadBufferSize    int
}

// DefaultConfig returns safe defaults for LAN use.
func DefaultConfig() Config {
	return Config{
		UDPPort:           26931,
		BroadcastInterval: 2 * time.Second,
		ReadBufferSize:    65536,
	}
}

// DiscoveredAdmin represents a discovered admin node.
type DiscoveredAdmin struct {
	Name    string
	IP      string
	TCPPort int
}

// DiscoveredPeer represents any discovered node.
type DiscoveredPeer struct {
	PeerID          string
	Name            string
	IP              string
	TCPPort         int
	Role            string
	ChunkBitmap     []byte
	TotalChunks     int
	PreloadedHashes []string
}

// ----- Network utilities (shared by discovery & app) ------------------------

var (
	localIPCache  []string
	localIPTTL    time.Time
	localIPMu     sync.RWMutex
	localCacheTTL = 10 * time.Second
)

// LocalIPs returns all non-loopback IPv4 addresses.
func LocalIPs() []string {
	now := time.Now()
	localIPMu.RLock()
	if now.Before(localIPTTL) && localIPCache != nil {
		result := localIPCache
		localIPMu.RUnlock()
		return result
	}
	localIPMu.RUnlock()

	localIPMu.Lock()
	defer localIPMu.Unlock()
	if now.Before(localIPTTL) && localIPCache != nil {
		return localIPCache
	}

	var ips []string
	ifaces, err := net.Interfaces()
	if err != nil {
		return ips
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if len(iface.HardwareAddr) == 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil {
				continue
			}
			ips = append(ips, ipNet.IP.String())
		}
	}
	localIPCache = ips
	localIPTTL = now.Add(localCacheTTL)
	return ips
}

// Hostname returns the machine hostname.
func Hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// IsLocalIP returns true if ip matches any local address.
func IsLocalIP(ip net.IP) bool {
	for _, lip := range LocalIPs() {
		if ip.Equal(net.ParseIP(lip)) {
			return true
		}
	}
	return false
}

// BroadcastAddr calculates the IPv4 broadcast address for a network.
func BroadcastAddr(ipNet *net.IPNet) net.IP {
	if ipNet == nil {
		return nil
	}
	ip := ipNet.IP.To4()
	if ip == nil {
		return nil
	}
	mask := ipNet.Mask
	if len(mask) != 4 {
		return nil
	}
	bcast := make(net.IP, 4)
	for i := 0; i < 4; i++ {
		bcast[i] = ip[i] | ^mask[i]
	}
	return bcast
}

type ifaceInfo struct {
	name  string
	addrs []*net.IPNet
}

func enumerateInterfaces() ([]ifaceInfo, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var result []ifaceInfo
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if len(iface.HardwareAddr) == 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		var ipNets []*net.IPNet
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil {
				continue
			}
			ipNets = append(ipNets, ipNet)
		}
		if len(ipNets) > 0 {
			result = append(result, ifaceInfo{name: iface.Name, addrs: ipNets})
		}
	}
	return result, nil
}

// bufferReader is a zero-allocation io.Reader over a byte slice.
type bufferReader struct {
	buf []byte
	pos int
}

func (r *bufferReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.buf) {
		return 0, fmt.Errorf("eof")
	}
	n := copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}
