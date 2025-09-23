package exceptions

type ApplicationErrorType string

const (
	ApplicationErrorTypeIrrecoverablePublicationMissing ApplicationErrorType = "irrecoverable_publication_missing"
	ApplicationErrorTypeIrrecoverableSlotMissing        ApplicationErrorType = "irrecoverable_slot_missing"
	ApplicationErrorTypeIrrecoverableInvalidSnapshot    ApplicationErrorType = "irrecoverable_invalid_snapshot"
)

func (a ApplicationErrorType) String() string {
	return string(a)
}
