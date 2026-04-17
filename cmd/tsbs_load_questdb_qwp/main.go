// tsbs_load_questdb_qwp replays a frame-only QWP dump (produced by
// tsbs_generate_data_questdb_qwp) against a live QuestDB server. It opens
// its own WebSocket upgrade against the HTTP port, validates the
// 101 Switching Protocols response, then streams the dump verbatim onto
// the upgraded connection.
package main

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const (
	qwpRequestPath = "/write/v4"
	qwpClientID    = "tsbs_load_questdb_qwp/1"
	qwpMaxVersion  = "1"
)

func main() {
	host := flag.String("host", "127.0.0.1", "QuestDB server host")
	port := flag.Int("port", 9000, "QuestDB HTTP port (QWP upgrades from HTTP)")
	file := flag.String("file", "", "Dump file to replay (empty = stdin)")
	useTLS := flag.Bool("tls", false, "Connect over TLS (wss://)")
	insecure := flag.Bool("tls-insecure-skip-verify", false, "Skip server certificate verification")
	drainTimeout := flag.Duration("drain-timeout", 5*time.Second,
		"Max time to wait for the server to close after the dump is sent")
	flag.Parse()

	in, closeIn, err := openInput(*file)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	defer closeIn()

	addr := net.JoinHostPort(*host, fmt.Sprintf("%d", *port))
	conn, err := dial(addr, *useTLS, *insecure)
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()

	start := time.Now()
	br, err := upgrade(conn, addr)
	if err != nil {
		log.Fatalf("upgrade: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, br)
		done <- err
	}()

	n, err := io.Copy(conn, in)
	if err != nil {
		log.Fatalf("send: %v", err)
	}

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}

	select {
	case err := <-done:
		if err != nil && err != io.EOF {
			log.Printf("drain: %v", err)
		}
	case <-time.After(*drainTimeout):
		log.Printf("drain timed out after %s", *drainTimeout)
	}

	elapsed := time.Since(start)
	throughput := float64(n) / elapsed.Seconds() / (1 << 20)
	fmt.Fprintf(os.Stderr, "sent %d bytes in %s (%.2f MiB/s)\n",
		n, elapsed.Round(time.Millisecond), throughput)
}

// upgrade performs the HTTP/1.1 WebSocket upgrade for the QWP endpoint
// and returns a bufio.Reader that wraps conn — the drain goroutine
// reads from it so any bytes the server sent after its response
// headers are not lost.
func upgrade(conn net.Conn, hostValue string) (*bufio.Reader, error) {
	key, err := makeWebSocketKey()
	if err != nil {
		return nil, fmt.Errorf("websocket key: %w", err)
	}
	req := "GET " + qwpRequestPath + " HTTP/1.1\r\n" +
		"Host: " + hostValue + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"X-QWP-Max-Version: " + qwpMaxVersion + "\r\n" +
		"X-QWP-Client-Id: " + qwpClientID + "\r\n" +
		"\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read status: %w", err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 101 ") {
		return nil, fmt.Errorf("unexpected status: %s", strings.TrimRight(status, "\r\n"))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read response headers: %w", err)
		}
		if line == "\r\n" {
			return br, nil
		}
	}
}

func makeWebSocketKey() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "" {
		return os.Stdin, func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}

func dial(addr string, useTLS, insecure bool) (net.Conn, error) {
	if !useTLS {
		return net.Dial("tcp", addr)
	}
	return tls.Dial("tcp", addr, &tls.Config{
		InsecureSkipVerify: insecure,
		MinVersion:         tls.VersionTLS12,
	})
}
