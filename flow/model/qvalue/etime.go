package qvalue

import (
	"errors"
	"time"
)

type ExtendedTimeKindType string

const (
	DateTimeKindType ExtendedTimeKindType = "datetime"
	DateKindType     ExtendedTimeKindType = "date"
	TimeKindType     ExtendedTimeKindType = "time"
)

type ExtendedTime struct {
	time.Time
	NestedKind ExtendedTimeKind
}

type ExtendedTimeKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
	DateTime = ExtendedTimeKind{
		Type:   DateTimeKindType,
		Format: time.RFC3339Nano,
	}

	Date = ExtendedTimeKind{
		Type:   DateKindType,
		Format: "2006-01-02",
	}

	Time = ExtendedTimeKind{
		Type:   TimeKindType,
		Format: "15:04:05.999999",
	}
)

func NewExtendedTime(
	t time.Time,
	kindType ExtendedTimeKindType,
	originalFormat string,
) (*ExtendedTime, error) {
	var nk ExtendedTimeKind

	switch kindType {
	case DateTimeKindType:
		nk = DateTime
	case DateKindType:
		nk = Date
	case TimeKindType:
		nk = Time
	default:
		return nil, errors.New("invalid ExtendedTimeKindType")
	}

	if originalFormat != "" {
		nk = ExtendedTimeKind{
			Type:   nk.Type,
			Format: originalFormat,
		}
	}

	return &ExtendedTime{
		Time:       t,
		NestedKind: nk,
	}, nil
}
