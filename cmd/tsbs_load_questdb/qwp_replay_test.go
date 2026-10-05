package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestQwpPostHandshakeWriter(t *testing.T) {
	var out bytes.Buffer
	writer := &qwpPostHandshakeWriter{inner: &out}
	parts := [][]byte{
		[]byte("GET /write/v4 HTTP/1.1\r\nHost: dump."),
		[]byte("local\r\n\r"),
		append([]byte("\n"), 0x82, 0x01, 0x2A),
		{0x82, 0x01, 0x2B},
	}
	for _, part := range parts {
		if n, err := writer.Write(part); err != nil || n != len(part) {
			t.Fatalf("Write returned n=%d err=%v", n, err)
		}
	}
	want := []byte{0x82, 0x01, 0x2A, 0x82, 0x01, 0x2B}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("post-handshake bytes:\n got  %x\n want %x", out.Bytes(), want)
	}
}

func TestReadQwpReplayResponses(t *testing.T) {
	var stream bytes.Buffer
	for sequence := uint64(0); sequence < 3; sequence++ {
		payload := make([]byte, 11)
		binary.LittleEndian.PutUint64(payload[1:9], sequence)
		stream.WriteByte(0x82)
		stream.WriteByte(byte(len(payload)))
		stream.Write(payload)
	}
	stream.Write([]byte{0x88, 0x00})

	acked, err := readQwpReplayResponses(bufio.NewReader(&stream))
	if err != nil {
		t.Fatalf("readQwpReplayResponses: %v", err)
	}
	if acked != 3 {
		t.Fatalf("acked frames: got %d want 3", acked)
	}
}

func TestReadQwpReplayResponsesRejectsErrorAck(t *testing.T) {
	message := []byte("bad batch")
	payload := make([]byte, 11+len(message))
	payload[0] = 0x01
	binary.LittleEndian.PutUint16(payload[9:11], uint16(len(message)))
	copy(payload[11:], message)

	var stream bytes.Buffer
	stream.WriteByte(0x82)
	stream.WriteByte(byte(len(payload)))
	stream.Write(payload)

	_, err := readQwpReplayResponses(bufio.NewReader(&stream))
	if err == nil || !bytes.Contains([]byte(err.Error()), message) {
		t.Fatalf("expected server error containing %q, got %v", message, err)
	}
}

func TestReadServerWebSocketFrameExtendedLength(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5A}, 130)
	var stream bytes.Buffer
	stream.Write([]byte{0x82, 126, 0, byte(len(payload))})
	stream.Write(payload)

	opcode, got, err := readServerWebSocketFrame(bufio.NewReader(&stream))
	if err != nil {
		t.Fatalf("readServerWebSocketFrame: %v", err)
	}
	if opcode != 0x2 || !bytes.Equal(got, payload) {
		t.Fatalf("frame mismatch: opcode=%x payload=%x", opcode, got)
	}
	if _, _, err := readServerWebSocketFrame(bufio.NewReader(bytes.NewReader(nil))); err != io.EOF {
		t.Fatalf("empty frame error: got %v want io.EOF", err)
	}
}

func TestReplayQwpDumpWaitsForFinalAck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		request, err := http.ReadRequest(reader)
		if err != nil {
			serverErr <- err
			return
		}
		key := request.Header.Get("Sec-WebSocket-Key")
		if _, err := fmt.Fprintf(
			conn,
			"HTTP/1.1 101 Switching Protocols\r\n"+
				"Upgrade: websocket\r\n"+
				"Connection: Upgrade\r\n"+
				"Sec-WebSocket-Accept: %s\r\n\r\n",
			webSocketAccept(key),
		); err != nil {
			serverErr <- err
			return
		}
		if _, err := io.ReadAll(reader); err != nil {
			serverErr <- err
			return
		}

		ack := make([]byte, 11)
		binary.LittleEndian.PutUint64(ack[1:9], 2)
		if _, err := conn.Write(append([]byte{0x82, byte(len(ack))}, ack...)); err != nil {
			serverErr <- err
			return
		}
		if _, err := conn.Write([]byte{0x88, 0x00}); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	path := t.TempDir() + "/frames"
	frames := []byte{
		0x82, 0x80, 0, 0, 0, 0,
		0x82, 0x80, 0, 0, 0, 0,
		0x82, 0x80, 0, 0, 0, 0,
	}
	if err := os.WriteFile(path, frames, 0o600); err != nil {
		t.Fatal(err)
	}

	oldTLS := useTLS
	useTLS = false
	defer func() { useTLS = oldTLS }()

	n, acked, err := replayQwpDump(listener.Addr().String(), path, 3, time.Second)
	if err != nil {
		t.Fatalf("replayQwpDump: %v", err)
	}
	if n != int64(len(frames)) || acked != 3 {
		t.Fatalf("replay result: bytes=%d acked=%d", n, acked)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
}
