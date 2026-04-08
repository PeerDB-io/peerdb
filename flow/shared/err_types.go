package shared

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

var (
	ErrSlotAlreadyExists = temporal.NewNonRetryableApplicationError("slot already exists",
		exceptions.ApplicationErrorTypeIrrecoverableExistingSlot.String(), nil)
	ErrTableDoesNotExist = temporal.NewNonRetryableApplicationError("table does not exist",
		exceptions.ApplicationErrorTypeIrrecoverableMissingTables.String(), nil)
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

func WrapError(s string, err error) error {
	var applicationError *temporal.ApplicationError

	if errors.As(err, &applicationError) {
		return temporal.NewNonRetryableApplicationError(s, applicationError.Type(), applicationError)
	} else {
		return fmt.Errorf("%s: %w", s, err)
	}
}
