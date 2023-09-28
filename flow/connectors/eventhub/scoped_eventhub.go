package conneventhub

import (
	"fmt"
	"strings"
)

type ScopedEventhub struct {
	PeerName string
	Eventhub string
}

func NewScopedEventhub(raw string) (ScopedEventhub, error) {
	// split by dot
	parts := strings.Split(raw, ".")

	if len(parts) != 2 {
		return ScopedEventhub{}, fmt.Errorf("invalid peer and topic name %s", raw)
	}

	return ScopedEventhub{
		PeerName: parts[0],
		Eventhub: parts[1],
	}, nil
}

func (s ScopedEventhub) Equals(other ScopedEventhub) bool {
	return s.PeerName == other.PeerName && s.Eventhub == other.Eventhub
}

// ToString returns the string representation of the ScopedEventhub
func (s ScopedEventhub) ToString() string {
	return fmt.Sprintf("%s.%s", s.PeerName, s.Eventhub)
}
