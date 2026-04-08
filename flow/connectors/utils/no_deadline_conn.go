package utils

import (
	"net"
	"time"
)

// see: https://github.com/jackc/pgx/issues/382#issuecomment-1496586216
type NoDeadlineConn struct{ net.Conn }

func (c *NoDeadlineConn) SetDeadline(t time.Time) error      { return nil }
func (c *NoDeadlineConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *NoDeadlineConn) SetWriteDeadline(t time.Time) error { return nil }
