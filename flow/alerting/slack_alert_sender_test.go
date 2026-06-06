package alerting

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatCCMembers(t *testing.T) {
	t.Parallel()

	//nolint:govet // it's a test, no need for fieldalignment
	tests := []struct {
		name    string
		members []string
		want    string
	}{
		{
			name:    "empty members pings whole channel",
			members: nil,
			want:    "cc: <!channel>",
		},
		{
			name:    "empty slice pings whole channel",
			members: []string{},
			want:    "cc: <!channel>",
		},
		{
			name:    "single member wrapped in Slack mention syntax",
			members: []string{"U01ABC234"},
			want:    "cc: <@U01ABC234>",
		},
		{
			name:    "multiple members each wrapped and space-separated",
			members: []string{"U01ABC234", "U05XYZ999"},
			want:    "cc: <@U01ABC234> <@U05XYZ999>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, formatCCMembers(tt.members))
		})
	}
}
