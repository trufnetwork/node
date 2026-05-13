package tn_vacuum

import "testing"

func TestCountRepackedTables(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   int
	}{
		{
			name: "no eligible tables",
			output: "INFO: database \"kwild_test_db\" skipped: pg_repack 1.5.3 is not installed in the database\n" +
				"INFO: database \"postgres\" skipped: pg_repack 1.5.3 is not installed in the database",
			want: 0,
		},
		{
			name:   "single table",
			output: "INFO: repacking table \"main\".\"primitive_events\"",
			want:   1,
		},
		{
			name: "multiple tables",
			output: "INFO: repacking table \"main\".\"primitive_events\"\n" +
				"INFO: repacking table \"main\".\"streams\"\n" +
				"INFO: repacking table \"main\".\"taxonomies\"",
			want: 3,
		},
		{
			name:   "empty output",
			output: "",
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := countRepackedTables(tt.output); got != tt.want {
				t.Fatalf("countRepackedTables() = %d, want %d", got, tt.want)
			}
		})
	}
}

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
