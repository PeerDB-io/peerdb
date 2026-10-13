package exceptions

type S3Error struct {
	error
}

func NewS3Error(err error) *S3Error {
	return &S3Error{err}
}

func (e *S3Error) Error() string {
	return "S3 Error: " + e.error.Error()
}

func (e *S3Error) Unwrap() error {
	return e.error
}
