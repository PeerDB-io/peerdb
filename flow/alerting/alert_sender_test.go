package alerting

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlackAlertSenderThresholds(t *testing.T) {
	t.Parallel()

	config := &slackAlertConfig{
		AuthToken:                                  "test-token",
		ChannelIDs:                                 []string{"channel1"},
		Members:                                    []string{"user1"},
		SlotLagMBAlertThreshold:                    1000,
		OpenConnectionsAlertThreshold:              10,
		IntervalSinceLastNormalizeMinutesThreshold: 120,
	}

	sender := newSlackAlertSender(config)

	assert.Equal(t, uint32(1000), sender.getSlotLagMBAlertThreshold())
	assert.Equal(t, uint32(10), sender.getOpenConnectionsAlertThreshold())
	assert.Equal(t, uint32(120), sender.getIntervalSinceLastNormalizeMinutesThreshold())
}

func TestSlackAlertSenderDefaultThresholds(t *testing.T) {
	t.Parallel()

	// Test with zero values (use global default)
	config := &slackAlertConfig{
		AuthToken:  "test-token",
		ChannelIDs: []string{"channel1"},
	}

	sender := newSlackAlertSender(config)

	assert.Equal(t, uint32(0), sender.getSlotLagMBAlertThreshold())
	assert.Equal(t, uint32(0), sender.getOpenConnectionsAlertThreshold())
	assert.Equal(t, uint32(0), sender.getIntervalSinceLastNormalizeMinutesThreshold())
}

func TestEmailAlertSenderThresholds(t *testing.T) {
	t.Parallel()

	config := &EmailAlertSenderConfig{
		EmailAddresses:                             []string{"test@example.com"},
		SlotLagMBAlertThreshold:                    2000,
		OpenConnectionsAlertThreshold:              20,
		IntervalSinceLastNormalizeMinutesThreshold: 180,
	}

	sender := NewEmailAlertSender(nil, config)

	assert.Equal(t, uint32(2000), sender.getSlotLagMBAlertThreshold())
	assert.Equal(t, uint32(20), sender.getOpenConnectionsAlertThreshold())
	assert.Equal(t, uint32(180), sender.getIntervalSinceLastNormalizeMinutesThreshold())
}

func TestEmailAlertSenderDefaultThresholds(t *testing.T) {
	t.Parallel()

	// Test with zero values (use global default)
	config := &EmailAlertSenderConfig{
		EmailAddresses: []string{"test@example.com"},
	}

	sender := NewEmailAlertSender(nil, config)

	assert.Equal(t, uint32(0), sender.getSlotLagMBAlertThreshold())
	assert.Equal(t, uint32(0), sender.getOpenConnectionsAlertThreshold())
	assert.Equal(t, uint32(0), sender.getIntervalSinceLastNormalizeMinutesThreshold())
}
