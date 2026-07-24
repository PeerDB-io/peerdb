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

type QRepWarnings []error

func WrapError(s string, err error) error {
	if applicationError, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return temporal.NewNonRetryableApplicationError(s, applicationError.Type(), applicationError)
	} else {
		return fmt.Errorf("%s: %w", s, err)
	}
}
