package shared

import (
	"go.temporal.io/sdk/temporal"
)

var (
	ErrSlotAlreadyExists error = temporal.NewNonRetryableApplicationError("slot already exists", "snapshot", nil)
	ErrTableDoesNotExist error = temporal.NewNonRetryableApplicationError("table does not exist", "snapshot", nil)
)

type ErrType string

const (
	ErrTypeCanceled ErrType = "err:Canceled"
	ErrTypeClosed   ErrType = "err:Closed"
	ErrTypeNet      ErrType = "err:Net"
	ErrTypeEOF      ErrType = "err:EOF"
)

func SkipSendingToIncidentIo(errTags []string) bool {
	skipTags := map[string]struct{}{
		string(ErrTypeCanceled): {},
		string(ErrTypeClosed):   {},
		string(ErrTypeNet):      {},
	}
	for _, tag := range errTags {
		if _, ok := skipTags[tag]; ok {
			return true
		}
	}
	return false
}

type QRepWarnings []error
