package common

import "fmt"

// gRPC ErrorInfo constants
const (
	ErrorInfoDomain = "peerdb.io"

	ErrorInfoReasonMirror             = "MIRROR"
	ErrorInfoReasonSourceTableMissing = "SOURCE_TABLE_MISSING"

	ErrorMetadataOffendingField = "offendingField"
	ErrorMetadataMissingTable   = "missingTable"
)

type SourceTableMissingError struct {
	Table QualifiedTable
}

func NewSourceTableMissingError(table QualifiedTable) *SourceTableMissingError {
	return &SourceTableMissingError{Table: table}
}

func (e *SourceTableMissingError) Error() string {
	return fmt.Sprintf("source table %s.%s does not exist", e.Table.Namespace, e.Table.Table)
}
