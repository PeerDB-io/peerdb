package wire

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestRoundTripFakeServer(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go fakeServer(t, server)

	c := New(client, client)
	hi, err := c.Hello()
	if err != nil {
		t.Fatal(err)
	}
	if hi.Engine != "mysql" || hi.Protocol != 1 {
		t.Fatalf("bad hello: %+v", hi)
	}
	raw, err := c.ParseBatch([]run.Case{{SQL: []byte("ALTER TABLE t ADD c INT"), SQLMode: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 1 || string(raw[0]) != `{"verdict":"reject","error":"fake"}` {
		t.Fatalf("bad parse response: %q", raw)
	}
	cov, err := c.GetCoverage()
	if err != nil {
		t.Fatal(err)
	}
	if string(cov) != "\x00\x01\x02" {
		t.Fatalf("bad coverage: %v", cov)
	}
}

func fakeServer(t *testing.T, rw io.ReadWriter) {
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
			hello, _ := json.Marshal(HelloInfo{Engine: "mysql", ServerVersion: "fake", Protocol: 1})
			resp := []byte{MsgHello}
			resp = binary.LittleEndian.AppendUint32(resp, uint32(len(hello)))
			resp = append(resp, hello...)
			writeFrame(t, rw, resp)
		case MsgParseBatch:
			resp := []byte{MsgParseBatch}
			resp = binary.LittleEndian.AppendUint32(resp, 1)
			d := []byte(`{"verdict":"reject","error":"fake"}`)
			resp = binary.LittleEndian.AppendUint32(resp, uint32(len(d)))
			resp = append(resp, d...)
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
