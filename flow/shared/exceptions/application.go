package exceptions

type ApplicationErrorType string

var IrrecoverableApplicationErrorTypesMap = make(map[string]struct{})

var IrrecoverableApplicationErrorTypesList = make([]string, 0)

func NewIrrecoverableApplicationErrorType(errorType string) ApplicationErrorType {
	IrrecoverableApplicationErrorTypesMap[errorType] = struct{}{}
	IrrecoverableApplicationErrorTypesList = append(IrrecoverableApplicationErrorTypesList, errorType)
	return ApplicationErrorType(errorType)
}

var (
	ApplicationErrorTypeIrrecoverablePublicationMissing = NewIrrecoverableApplicationErrorType("irrecoverable_publication_missing")
	ApplicationErrorTypeIrrecoverableSlotMissing        = NewIrrecoverableApplicationErrorType("irrecoverable_slot_missing")
	ApplicationErrorTypeIrrecoverableInvalidSnapshot    = NewIrrecoverableApplicationErrorType("irrecoverable_invalid_snapshot")
	ApplicationErrorTypeIrrecoverableExistingSlot       = NewIrrecoverableApplicationErrorType("irrecoverable_existing_slot")
	ApplicationErrorTypeIrrecoverableMissingTables      = NewIrrecoverableApplicationErrorType("irrecoverable_missing_tables")
)

func (a ApplicationErrorType) String() string {
	return string(a)
}
