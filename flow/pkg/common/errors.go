package common

import (
	"fmt"
	"strings"
)

// gRPC ErrorInfo constants
const (
	ErrorInfoDomain = "peerdb.io"
	// MaxTablesInErrorDetails bounds table lists carried in gRPC status metadata.
	// Rich gRPC errors are transported in response headers/trailers, which have
	// substantially smaller limits than response bodies.
	MaxTablesInErrorDetails = 20

	ErrorInfoReasonMirror                 = "MIRROR"
	ErrorInfoReasonSourceTableMissing     = "SOURCE_TABLE_MISSING"
	ErrorInfoReasonTablesNotInPublication = "TABLES_NOT_IN_PUBLICATION"
	ErrorInfoReasonSourceValidationFailed = "SOURCE_VALIDATION_FAILED"

	ErrorMetadataOffendingField = "offendingField"
	ErrorMetadataPublication    = "publication"
	ErrorMetadataTableCount     = "tableCount"
)

type SourceTablesMissingError struct {
	Tables []QualifiedTable
}

func NewSourceTablesMissingError(tables []QualifiedTable) *SourceTablesMissingError {
	return &SourceTablesMissingError{Tables: tables}
}

func (e *SourceTablesMissingError) Error() string {
	return "source tables do not exist: " + formatTablesForError(e.Tables)
}

type TablesNotInPublicationError struct {
	Publication string
	Tables      []QualifiedTable
}

func NewTablesNotInPublicationError(publication string, tables []QualifiedTable) *TablesNotInPublicationError {
	return &TablesNotInPublicationError{Publication: publication, Tables: tables}
}

func (e *TablesNotInPublicationError) Error() string {
	return fmt.Sprintf("tables not in publication %q: %s", e.Publication, formatTablesForError(e.Tables))
}

func formatTablesForError(tables []QualifiedTable) string {
	limit := min(len(tables), MaxTablesInErrorDetails)
	parts := make([]string, limit)
	for i, t := range tables[:limit] {
		parts[i] = fmt.Sprintf("%s.%s", t.Namespace, t.Table)
	}
	if omitted := len(tables) - limit; omitted > 0 {
		parts = append(parts, fmt.Sprintf("and %d more", omitted))
	}
	return strings.Join(parts, ", ")
}

type ReplicaIdentifierInUseError struct {
	Id string
}

func NewReplicaIdentifierInUseError(id string) *ReplicaIdentifierInUseError {
	return &ReplicaIdentifierInUseError{Id: id}
}

func (e *ReplicaIdentifierInUseError) Error() string {
	return fmt.Sprintf("replica identifier %q is already in use by a replica registered on the source database", e.Id)
}
