package shared

import "errors"

var (
	ErrSlotAlreadyExists error = errors.New("slot already exists")
	ErrTableDoesNotExist error = errors.New("table does not exist")
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
