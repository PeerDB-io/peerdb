package utils

import (
	"fmt"
	"os"
)

func GetAzureSubscriptionID() (string, error) {
	// get this from env
	id := os.Getenv("AZURE_SUBSCRIPTION_ID")
	if id == "" {
		return "", fmt.Errorf("AZURE_SUBSCRIPTION_ID is not set")
	}
	return id, nil
}
