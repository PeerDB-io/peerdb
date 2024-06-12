package middleware

import (
	"context"
	"encoding/base64"
	"log/slog"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/PeerDB-io/peer-flow/logger"
)

// authorize checks the authorization metadata and compares the incoming bearer token with the plaintext
func authorize(ctx context.Context, hash []byte) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md["authorization"]) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "Authorization token is required")
	}
	headerValue := md["authorization"][0]
	base64Token, hasPrefix := strings.CutPrefix(headerValue, "Bearer ")
	if !hasPrefix {
		return nil, status.Errorf(codes.Unauthenticated, "Unsupported authorization type")
	} else if base64Token == "" {
		return nil, status.Errorf(codes.Unauthenticated, "Authorization token is required")
	}
	// Always a good practice to have the actual token in base64
	tokenBytes, err := base64.StdEncoding.DecodeString(base64Token)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("Error decoding token", slog.Any("error", err))
		return nil, status.Errorf(codes.Unauthenticated, "Authentication failed")
	}

	if err := bcrypt.CompareHashAndPassword(hash, tokenBytes); err != nil {
		logger.LoggerFromCtx(ctx).Warn("Error validating token", slog.Any("error", err))
		return nil, status.Errorf(codes.Unauthenticated, "Authentication failed")
	}
	return ctx, nil
}
