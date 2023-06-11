package model

import "time"

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
	// DateTime represents the NestedKind for datetime objects, using RFC3339Nano format for timestamps
	DateTime = NestedKind{
		Type:   DateTimeKindType,
		Format: time.RFC3339Nano,
	}

	// Date represents the NestedKind for date objects, using "2006-01-02" format (equivalent to yyyy-mm-dd)
	Date = NestedKind{
		Type:   DateKindType,
		Format: "2006-01-02",
	}

	// Time represents the NestedKind for time objects, using "15:04:05.999999" format (equivalent to hh:mm:ss.ffffff)
	Time = NestedKind{
		Type:   TimeKindType,
		Format: "15:04:05.999999",
	}
)

func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) *ExtendedTime {
	var nk NestedKind

	switch kindType {
	case DateTimeKindType:
		nk = DateTime
	case DateKindType:
		nk = Date
	case TimeKindType:
		nk = Time
	}

	if originalFormat != "" {
		nk.Format = originalFormat
	}

	return &ExtendedTime{
		Time:       t,
		NestedKind: nk,
	}
}
