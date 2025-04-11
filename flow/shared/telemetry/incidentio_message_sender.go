package telemetry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type IncidentIoAlert struct {
	Metadata         map[string]string `json:"metadata"`
	Title            string            `json:"title"`
	Description      string            `json:"description"`
	DeduplicationKey string            `json:"deduplication_key"`
	Status           string            `json:"status"`
}

type IncidentIoResponse struct {
	Status           string `json:"status"`
	Message          string `json:"message"`
	DeduplicationKey string `json:"deduplication_key"`
}

type IncidentIoMessageSender struct {
	http   *http.Client
	config IncidentIoMessageSenderConfig
}

type IncidentIoMessageSenderConfig struct {
	URL   string
	Token string
}

func (i *IncidentIoMessageSender) SendMessage(
	ctx context.Context,
	subject string,
	body string,
	attributes Attributes,
) (string, error) {
	activityInfo := activity.Info{}
	if activity.IsActivity(ctx) {
		activityInfo = activity.GetInfo(ctx)
	}

	if shared.SkipSendingToIncidentIo(attributes.Tags) {
		logger := internal.LoggerFromCtx(ctx)
		logger.Info("skipping incident.io alert",
			slog.Any("attributes", attributes),
			slog.String("subject", subject),
			slog.String("body", body))
		return "", nil
	}

	deduplicationString := strings.Join([]string{
		"deployID", attributes.DeploymentUID,
		"subject", subject,
		"runID", activityInfo.WorkflowExecution.RunID,
		"activityName", activityInfo.ActivityType.Name,
	}, " || ")
	h := sha256.New()
	h.Write([]byte(deduplicationString))
	deduplicationHash := hex.EncodeToString(h.Sum(nil))

	level := ResolveIncidentIoLevels(attributes.Level)

	alertMetadata := map[string]string{
		"alias":          deduplicationHash,
		"deploymentUUID": attributes.DeploymentUID,
		"entity":         attributes.DeploymentUID,
		"level":          string(level),
		"tags":           strings.Join(attributes.Tags, ","),
		"type":           attributes.Type,
	}

	flowMetadata := internal.GetFlowMetadata(ctx)
	if flowMetadata != nil {
		maps.Copy(alertMetadata, map[string]string{
			"flowName":            flowMetadata.FlowName,
			"sourcePeerType":      flowMetadata.Source.Type.String(),
			"destinationPeerType": flowMetadata.Destination.Type.String(),
			"sourcePeerName":      flowMetadata.Source.Name,
			"destinationPeerName": flowMetadata.Destination.Name,
			"flowStatus":          flowMetadata.Status.String(),
			"isResync":            strconv.FormatBool(flowMetadata.IsResync),
		})
	}

	alert := IncidentIoAlert{
		Title:            subject,
		Description:      body,
		DeduplicationKey: deduplicationHash,
		Status:           "firing",
		Metadata:         alertMetadata,
	}

	alertJSON, err := json.Marshal(alert)
	if err != nil {
		return "", fmt.Errorf("error serializing alert %w", err)
	}

	req, err := http.NewRequest("POST", i.config.URL, bytes.NewBuffer(alertJSON))
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+i.config.Token)

	resp, err := i.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("incident.io request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading incident.io response body failed: %w", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("unexpected response from incident.io. status: %d. body: %s", resp.StatusCode, respBody)
	}

	var incidentResponse IncidentIoResponse
	err = json.Unmarshal(respBody, &incidentResponse)
	if err != nil {
		return "", fmt.Errorf("deserializing incident.io failed: %w", err)
	}

	return incidentResponse.Status, nil
}

func NewIncidentIoMessageSender(_ context.Context, config IncidentIoMessageSenderConfig) (*IncidentIoMessageSender, error) {
	client := &http.Client{
		Timeout: time.Second * 5,
	}

	return &IncidentIoMessageSender{
		config: config,
		http:   client,
	}, nil
}
