package main

import (
	"fmt"
	"log"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Generate flow config converter
	if err := generateFlowConfigConverter(); err != nil {
		return fmt.Errorf("failed to generate flow config converter: %w", err)
	}

	// Generate gRPC wrapper
	if err := generateGRPCWrapper(); err != nil {
		return fmt.Errorf("failed to generate gRPC wrapper: %w", err)
	}

	return nil
}
