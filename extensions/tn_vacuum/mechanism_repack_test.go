package tn_vacuum

import "testing"

func TestDetectPgRepackSoftFailure(t *testing.T) {
	tests := []struct {
		name    string
		stderr  string
		expects bool
	}{
		{
			name:    "version mismatch",
			stderr:  "INFO: database \"kwild\" skipped: program 'pg_repack 1.5.0' does not match database library 'pg_repack 1.5.2'",
			expects: true,
		},
		{
			name:    "extension missing",
			stderr:  "INFO: database \"kwild\" skipped: pg_repack 1.5.0 is not installed in the database",
			expects: false,
		},
		{
			name:    "no issues",
			stderr:  "INFO: repacking database \"kwild\"\nINFO: repacking table \"public\".\"foo\"",
			expects: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := detectPgRepackSoftFailure(tt.stderr)
			if tt.expects && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.expects && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
