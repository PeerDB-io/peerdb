package shared

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/temporal"
)

var (
	ErrSlotAlreadyExists error = temporal.NewNonRetryableApplicationError("slot already exists", "snapshot", nil)
	ErrTableDoesNotExist error = temporal.NewNonRetryableApplicationError("table does not exist", "snapshot", nil)
)

type ErrReplicaIdentityNothing struct {
	Table string
	Cause error
}

func (e *ErrReplicaIdentityNothing) Error() string {
	return fmt.Sprintf("table %s has replica identity 'n'/NOTHING", e.Table)
}

func (e *ErrReplicaIdentityNothing) Unwrap() error {
	return e.Cause
}

func NewErrReplicaIdentityNothing(table string, cause error) error {
	return &ErrReplicaIdentityNothing{Table: table, Cause: cause}
}

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
