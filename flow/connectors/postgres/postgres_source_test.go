package connpostgres

import (
	"context"
	"testing"
	"time"
)

func TestWalSenderTimeoutOverride(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		value    string
		duration time.Duration
		isSet    bool
	}{
		{"2min", 2 * time.Minute, true},
		{"120 s", 2 * time.Minute, true},
		{"120s", 2 * time.Minute, true},
		{"120000", 2 * time.Minute, true},
		{"0", 0, true},
		{"NONE", 0, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Parallel()

			got, err := walSenderTimeoutOverride(context.Background(), map[string]string{
				"PEERDB_POSTGRES_WAL_SENDER_TIMEOUT": tc.value,
			})
			if err != nil {
				t.Fatal(err)
			}
			want := walSenderTimeout{str: tc.value, duration: tc.duration, isSet: tc.isSet}
			if got != want {
				t.Errorf("walSenderTimeoutOverride() = %+v, want %+v", got, want)
			}
		})
	}
}
