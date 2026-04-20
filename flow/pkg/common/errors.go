package common

import (
	"fmt"
	"strings"
)

// gRPC ErrorInfo constants
const (
	ErrorInfoDomain = "peerdb.io"

	ErrorInfoReasonMirror                 = "MIRROR"
	ErrorInfoReasonSourceTableMissing     = "SOURCE_TABLE_MISSING"
	ErrorInfoReasonTablesNotInPublication = "TABLES_NOT_IN_PUBLICATION"

	ErrorMetadataOffendingField = "offendingField"
	ErrorMetadataPublication    = "publication"
)

type SourceTablesMissingError struct {
	Tables []QualifiedTable
}

func NewSourceTablesMissingError(tables []QualifiedTable) *SourceTablesMissingError {
	return &SourceTablesMissingError{Tables: tables}
}

func (e *SourceTablesMissingError) Error() string {
	parts := make([]string, len(e.Tables))
	for i, t := range e.Tables {
		parts[i] = fmt.Sprintf("%s.%s", t.Namespace, t.Table)
	}
	return "source tables do not exist: " + strings.Join(parts, ", ")
}

type TablesNotInPublicationError struct {
	Tables      []QualifiedTable
	Publication string
}

func NewTablesNotInPublicationError(publication string, tables []QualifiedTable) *TablesNotInPublicationError {
	return &TablesNotInPublicationError{Publication: publication, Tables: tables}
}

func (e *TablesNotInPublicationError) Error() string {
	parts := make([]string, len(e.Tables))
	for i, t := range e.Tables {
		parts[i] = fmt.Sprintf("%s.%s", t.Namespace, t.Table)
	}
	return fmt.Sprintf("tables not in publication %q: %s", e.Publication, strings.Join(parts, ", "))
}
