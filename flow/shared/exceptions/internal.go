package exceptions

import "fmt"

type CatalogError struct {
	error
}

func NewCatalogError(err error) *CatalogError {
	return &CatalogError{err}
}

func (e *CatalogError) Unwrap() error {
	return e.error
}

type DynamicConfError struct {
	error
}

func NewDynamicConfError(format string, a ...any) *DynamicConfError {
	return &DynamicConfError{fmt.Errorf(format, a...)}
}

func (e *DynamicConfError) Error() string {
	return "Dynamic Conf Error: " + e.error.Error()
}

func (e *DynamicConfError) Unwrap() error {
	return e.error
}

type ProtoMarshalError struct {
	error
}

func NewProtoMarshalError(err error) *ProtoMarshalError {
	return &ProtoMarshalError{err}
}

func (e *ProtoMarshalError) Error() string {
	return "Proto Marshal Error: " + e.error.Error()
}

type ProtoUnmarshalError struct {
	error
}

func NewProtoUnmarshalError(err error) *ProtoUnmarshalError {
	return &ProtoUnmarshalError{err}
}

func (e *ProtoUnmarshalError) Error() string {
	return "Proto Unmarshal Error: " + e.error.Error()
}

func (e *ProtoUnmarshalError) Unwrap() error {
	return e.error
}

type DecryptError struct {
	error
}

func NewDecryptError(err error) *DecryptError {
	return &DecryptError{err}
}

func (e *DecryptError) Error() string {
	return "Decrypt Error: " + e.error.Error()
}

func (e *DecryptError) Unwrap() error {
	return e.error
}
