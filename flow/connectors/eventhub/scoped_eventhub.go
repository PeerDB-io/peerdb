package conneventhub

import (
	"fmt"
	"strings"
)

type ScopedEventhub struct {
	PeerName   string
	Eventhub   string
	Identifier string
}

func NewScopedEventhub(raw string) (ScopedEventhub, error) {
	// split by dot, the model is peername.eventhub.identifier
	parts := strings.Split(raw, ".")

	if len(parts) != 3 {
		return ScopedEventhub{}, fmt.Errorf("invalid scoped eventhub '%s'", raw)
	}

	// support eventhub name with hyphens etc.
	eventhubPart := strings.Trim(parts[1], `"`)

	return ScopedEventhub{
		PeerName:   parts[0],
		Eventhub:   eventhubPart,
		Identifier: parts[2],
	}, nil
}

func (s ScopedEventhub) Equals(other ScopedEventhub) bool {
	return s.PeerName == other.PeerName &&
		s.Eventhub == other.Eventhub &&
		s.Identifier == other.Identifier
}

// ToString returns the string representation of the ScopedEventhub
func (s ScopedEventhub) ToString() string {
	return fmt.Sprintf("%s.%s.%s", s.PeerName, s.Eventhub, s.Identifier)
}
