package wire

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

const (
	MsgParseBatch  byte = 1
	MsgGetCoverage byte = 2
	MsgHello       byte = 3
)

// ProtocolVersion is the oracle wire protocol both sides must speak; the
// oracles report theirs in HELLO and every client (here and the supervisor's
// HelloSmoke) rejects a mismatch. Bump on any incompatible frame change.
const ProtocolVersion = 2

type HelloInfo struct {
	Engine        string `json:"engine"`
	ServerVersion string `json:"server_version"`
	Protocol      int    `json:"protocol"`
}

type Conn struct {
	w    *bufio.Writer
	r    *bufio.Reader
	hdr  [4]byte
	body []byte
	req  []byte
}

func New(r io.Reader, w io.Writer) *Conn {
	return &Conn{
		w: bufio.NewWriterSize(w, 1<<20),
		r: bufio.NewReaderSize(r, 1<<20),
	}
}

// ParseBatch returns, per case, the oracle's digest bytes and the number of
// virgin sancov edges that case opened in the oracle process.
func (c *Conn) ParseBatch(cases []run.Case) ([][]byte, []uint32, error) {
	c.req = c.req[:0]
	c.req = append(c.req, MsgParseBatch)
	c.req = binary.LittleEndian.AppendUint32(c.req, uint32(len(cases)))
	for _, tc := range cases {
		c.req = binary.LittleEndian.AppendUint64(c.req, tc.SQLMode)
		c.req = binary.LittleEndian.AppendUint32(c.req, uint32(len(tc.SQL)))
		c.req = append(c.req, tc.SQL...)
	}
	body, err := c.roundTrip(MsgParseBatch, c.req)
	if err != nil {
		return nil, nil, err
	}
	if len(body) < 4 {
		return nil, nil, io.ErrUnexpectedEOF
	}
	count := int(binary.LittleEndian.Uint32(body[:4]))
	body = body[4:]
	out := make([][]byte, 0, count)
	edges := make([]uint32, 0, count)
	for i := 0; i < count; i++ {
		if len(body) < 8 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		edges = append(edges, binary.LittleEndian.Uint32(body[:4]))
		n := int(binary.LittleEndian.Uint32(body[4:8]))
		body = body[8:]
		if n < 0 || len(body) < n {
			return nil, nil, io.ErrUnexpectedEOF
		}
		out = append(out, body[:n])
		body = body[n:]
	}
	if count != len(cases) {
		return nil, nil, fmt.Errorf("wire: parse response count %d != request count %d", count, len(cases))
	}
	return out, edges, nil
}

func (c *Conn) GetCoverage() ([]byte, error) {
	body, err := c.roundTrip(MsgGetCoverage, []byte{MsgGetCoverage})
	if err != nil {
		return nil, err
	}
	if len(body) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	n := int(binary.LittleEndian.Uint32(body[:4]))
	body = body[4:]
	if n < 0 || len(body) < n {
		return nil, io.ErrUnexpectedEOF
	}
	return body[:n], nil
}

func (c *Conn) Hello() (HelloInfo, error) {
	body, err := c.roundTrip(MsgHello, []byte{MsgHello})
	if err != nil {
		return HelloInfo{}, err
	}
	if len(body) < 4 {
		return HelloInfo{}, io.ErrUnexpectedEOF
	}
	n := int(binary.LittleEndian.Uint32(body[:4]))
	body = body[4:]
	if n < 0 || len(body) < n {
		return HelloInfo{}, io.ErrUnexpectedEOF
	}
	var hi HelloInfo
	if err := json.Unmarshal(body[:n], &hi); err != nil {
		return HelloInfo{}, err
	}
	if hi.Protocol != ProtocolVersion {
		return HelloInfo{}, fmt.Errorf("wire: unsupported protocol %d", hi.Protocol)
	}
	return hi, nil
}

func (c *Conn) roundTrip(msg byte, req []byte) ([]byte, error) {
	binary.LittleEndian.PutUint32(c.hdr[:], uint32(len(req)))
	if _, err := c.w.Write(c.hdr[:]); err != nil {
		return nil, err
	}
	if _, err := c.w.Write(req); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(c.r, c.hdr[:]); err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(c.hdr[:]))
	if n <= 0 {
		return nil, fmt.Errorf("wire: empty response frame")
	}
	if cap(c.body) < n {
		c.body = make([]byte, n)
	} else {
		c.body = c.body[:n]
	}
	if _, err := io.ReadFull(c.r, c.body); err != nil {
		return nil, err
	}
	if c.body[0] != msg {
		return nil, fmt.Errorf("wire: response msg %d != request msg %d", c.body[0], msg)
	}
	return c.body[1:], nil
}
