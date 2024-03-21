package utils

import (
	"errors"
	"os"
)

func GetAzureSubscriptionID() (string, error) {
	// get this from env
	id := os.Getenv("AZURE_SUBSCRIPTION_ID")
	if id == "" {
		return "", errors.New("AZURE_SUBSCRIPTION_ID is not set")
	}
	return id, nil
}
