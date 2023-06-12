package model

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
	NestedKind NestedKind
}

type NestedKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
	DateTime = NestedKind{
		Type:   DateTimeKindType,
		Format: time.RFC3339Nano,
	}

	Date = NestedKind{
		Type:   DateKindType,
		Format: "2006-01-02",
	}

	Time = NestedKind{
		Type:   TimeKindType,
		Format: "15:04:05.999999",
	}
)

func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) (*ExtendedTime, error) {
	var nk NestedKind

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
		nk = NestedKind{
			Type:   nk.Type,
			Format: originalFormat,
		}
	}

	return &ExtendedTime{
		Time:       t,
		NestedKind: nk,
	}, nil
}
