package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

type UserActionType int

// attribute to make efficient filtering for user action logs
const userActionTag = "userAction"

const (
	UserActionCreateFlow UserActionType = iota
	UserActionResyncFlow
	UserActionPauseFlow
	UserActionResumeFlow
	UserActionTerminateFlow
)

func (t UserActionType) String() string {
	switch t {
	case UserActionCreateFlow:
		return "create_flow"
	case UserActionResyncFlow:
		return "resync_flow"
	case UserActionPauseFlow:
		return "pause_flow"
	case UserActionResumeFlow:
		return "resume_flow"
	case UserActionTerminateFlow:
		return "terminate_flow"
	default:
		return "unknown"
	}
}

func logUserAction(ctx context.Context, actionType UserActionType, flowName string, actionDetailedMsg string, extraAttrs ...any) {
	requestID, _ := ctx.Value(shared.RequestIdKey).(string)
	attrs := []any{
		slog.String("logType", userActionTag),
		slog.String("flowName", flowName),
		slog.String("actionType", actionType.String()),
		slog.String("requestId", requestID),
	}
	attrs = append(attrs, extraAttrs...)
	msg := "[user action] " + actionType.String()
	if actionDetailedMsg != "" {
		msg += fmt.Sprintf("(%s)", actionDetailedMsg)
	}
	slog.InfoContext(ctx, msg, attrs...)
}

func LogActionCreateFlow(ctx context.Context, flowName string) {
	logUserAction(ctx, UserActionCreateFlow, flowName, "")
}

func LogActionResyncFlow(ctx context.Context, flowName string) {
	logUserAction(ctx, UserActionResyncFlow, flowName, "")
}

func LogActionPauseFlow(ctx context.Context, flowName string) {
	logUserAction(ctx, UserActionPauseFlow, flowName, "")
}

func LogActionResumeFlow(ctx context.Context, flowName string, tablesAdded, tablesRemoved []string) {
	var extraAttrs []any
	var logDetails []string
	if len(tablesAdded) > 0 {
		extraAttrs = append(extraAttrs, slog.Any("tablesAdded", tablesAdded))
		logDetails = append(logDetails, "tables added")
	}
	if len(tablesRemoved) > 0 {
		extraAttrs = append(extraAttrs, slog.Any("tablesRemoved", tablesRemoved))
		logDetails = append(logDetails, "tables removed")
	}
	logUserAction(ctx, UserActionResumeFlow, flowName, strings.Join(logDetails, ","), extraAttrs...)
}

func LogActionTerminateFlow(ctx context.Context, flowName string) {
	logUserAction(ctx, UserActionTerminateFlow, flowName, "")
}
