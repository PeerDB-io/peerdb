package utils

import (
	"bytes"
	"os"

	"github.com/sirupsen/logrus"
)

// we do this as logrus does not separate output into stderr and stdout
type LogRouter struct{}

func (router *LogRouter) Write(p []byte) (n int, err error) {
	if bytes.Contains(p, []byte("level=error")) {
		return os.Stderr.Write(p)
	}
	return os.Stdout.Write(p)
}

func init() {
	logrus.SetOutput(&LogRouter{})
}
