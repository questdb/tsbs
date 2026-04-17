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
	file := flag.String("file", "", "Dump file to replay (empty = stdin). With --parallel > 1 this is a prefix; inputs are {file}.0..{file}.N-1")
	useTLS := flag.Bool("tls", false, "Connect over TLS (wss://)")
	insecure := flag.Bool("tls-insecure-skip-verify", false, "Skip server certificate verification")
	parallel := flag.Int("parallel", 1, "Number of parallel WebSocket connections; when >1, reads {file}.0..{file}.N-1")
	drainTimeout := flag.Duration("drain-timeout", 5*time.Second,
		"Max time to wait for the server to close after the dump is sent")
	flag.Parse()

	if *parallel < 1 {
		log.Fatalf("--parallel must be >= 1, got %d", *parallel)
	}
	paths, err := inputPaths(*file, *parallel)
	if err != nil {
		log.Fatalf("input: %v", err)
	}

	addr := net.JoinHostPort(*host, fmt.Sprintf("%d", *port))

	type result struct {
		bytes int64
		err   error
	}
	results := make(chan result, len(paths))
	start := time.Now()
	for _, p := range paths {
		p := p
		go func() {
			n, err := replay(addr, *useTLS, *insecure, p, *drainTimeout)
			results <- result{bytes: n, err: err}
		}()
	}

	var total int64
	var firstErr error
	for i := 0; i < len(paths); i++ {
		r := <-results
		total += r.bytes
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
	}

	elapsed := time.Since(start)
	throughput := float64(total) / elapsed.Seconds() / (1 << 20)
	fmt.Fprintf(os.Stderr, "sent %d bytes across %d connection(s) in %s (%.2f MiB/s)\n",
		total, len(paths), elapsed.Round(time.Millisecond), throughput)
	if firstErr != nil {
		log.Fatalf("replay: %v", firstErr)
	}
}

func inputPaths(path string, parallel int) ([]string, error) {
	if parallel <= 1 {
		return []string{path}, nil
	}
	if path == "" {
		return nil, fmt.Errorf("--file is required when --parallel > 1")
	}
	out := make([]string, parallel)
	for i := 0; i < parallel; i++ {
		out[i] = fmt.Sprintf("%s.%d", path, i)
	}
	return out, nil
}

func replay(addr string, useTLS, insecure bool, path string, drainTimeout time.Duration) (int64, error) {
	in, closeIn, err := openInput(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer closeIn()

	conn, err := dial(addr, useTLS, insecure)
	if err != nil {
		return 0, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	br, err := upgrade(conn, addr)
	if err != nil {
		return 0, fmt.Errorf("upgrade: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, br)
		done <- err
	}()

	n, err := io.Copy(conn, in)
	if err != nil {
		return n, fmt.Errorf("send %s: %w", path, err)
	}

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}

	select {
	case err := <-done:
		if err != nil && err != io.EOF {
			log.Printf("drain %s: %v", path, err)
		}
	case <-time.After(drainTimeout):
		log.Printf("drain %s timed out after %s", path, drainTimeout)
	}
	return n, nil
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
