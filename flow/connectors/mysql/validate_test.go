package connmysql

import (
	"testing"
)

func TestCheckGrant(t *testing.T) {
	// Test cases for checkGrantCommand function
	tests := []struct {
		name      string
		command   []byte
		expectErr bool
	}{
		{
			name:      "Valid Grant Command with REPLICATION CLIENT",
			command:   []byte("GRANT REPLICATION CLIENT ON *.* TO `user`@`host`"),
			expectErr: false,
		},
		{
			name:      "Valid Grant Command with REPLICATION SLAVE",
			command:   []byte("GRANT SELECT, REPLICATION SLAVE, EXECUTE ON *.* TO `user`@`host`"),
			expectErr: false,
		},
		{
			name:      "Invalid Grant Command without replication privileges",
			command:   []byte("GRANT SELECT, INSERT ON *.* TO 'user'@'host'"),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkGrantCommand(tt.command)
			if (err != nil) != tt.expectErr {
				t.Errorf("checkGrantCommand() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}
