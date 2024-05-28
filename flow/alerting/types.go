package alerting

type ServiceType string

const (
	SLACK ServiceType = "slack"
	EMAIL ServiceType = "email"
)
