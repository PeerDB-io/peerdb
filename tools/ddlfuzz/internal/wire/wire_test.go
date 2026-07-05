package wire

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestRoundTripFakeServer(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go fakeServer(t, server, 2)

	c := New(client, client)
	hi, err := c.Hello()
	if err != nil {
		t.Fatal(err)
	}
	if hi.Engine != "mysql" || hi.Protocol != 2 {
		t.Fatalf("bad hello: %+v", hi)
	}
	raw, edges, err := c.ParseBatch([]run.Case{
		{SQL: []byte("ALTER TABLE t ADD c INT"), SQLMode: 4},
		{SQL: []byte("RENAME TABLE t TO u"), SQLMode: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2 || string(raw[0]) != `{"verdict":"reject","error":"fake 0"}` ||
		string(raw[1]) != `{"verdict":"reject","error":"fake 1"}` {
		t.Fatalf("bad parse response: %q", raw)
	}
	if len(edges) != 2 || edges[0] != 7 || edges[1] != 0 {
		t.Fatalf("bad edge counts: %v", edges)
	}
	cov, err := c.GetCoverage()
	if err != nil {
		t.Fatal(err)
	}
	if string(cov) != "\x00\x01\x02" {
		t.Fatalf("bad coverage: %v", cov)
	}
}

func TestHelloRejectsOldProtocol(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go fakeServer(t, server, 1)

	c := New(client, client)
	_, err := c.Hello()
	if err == nil || !strings.Contains(err.Error(), "unsupported protocol 1") {
		t.Fatalf("want unsupported-protocol error, got %v", err)
	}
}

func fakeServer(t *testing.T, rw io.ReadWriter, protocol int) {
	t.Helper()
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(rw, hdr[:]); err != nil {
			return
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		body := make([]byte, n)
		if _, err := io.ReadFull(rw, body); err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		switch body[0] {
		case MsgHello:
			hello, _ := json.Marshal(HelloInfo{Engine: "mysql", ServerVersion: "fake", Protocol: protocol})
			resp := []byte{MsgHello}
			resp = binary.LittleEndian.AppendUint32(resp, uint32(len(hello)))
			resp = append(resp, hello...)
			writeFrame(t, rw, resp)
		case MsgParseBatch:
			count := binary.LittleEndian.Uint32(body[1:5])
			resp := []byte{MsgParseBatch}
			resp = binary.LittleEndian.AppendUint32(resp, count)
			for i := uint32(0); i < count; i++ {
				var edges uint32
				if i == 0 {
					edges = 7
				}
				d := []byte(`{"verdict":"reject","error":"fake ` + string(rune('0'+i)) + `"}`)
				resp = binary.LittleEndian.AppendUint32(resp, edges)
				resp = binary.LittleEndian.AppendUint32(resp, uint32(len(d)))
				resp = append(resp, d...)
			}
			writeFrame(t, rw, resp)
		case MsgGetCoverage:
			resp := []byte{MsgGetCoverage}
			resp = binary.LittleEndian.AppendUint32(resp, 3)
			resp = append(resp, 0, 1, 2)
			writeFrame(t, rw, resp)
		}
	}
}

func writeFrame(t *testing.T, w io.Writer, body []byte) {
	t.Helper()
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		t.Error(err)
	}
	if _, err := w.Write(body); err != nil {
		t.Error(err)
	}
}
