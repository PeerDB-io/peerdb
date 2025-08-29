package exceptions

type MongoRetryablePoolError interface {
	error
	Retryable() bool
}
