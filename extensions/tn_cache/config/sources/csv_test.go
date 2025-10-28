package sources

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/trufnetwork/kwil-db/core/log"
)

func TestCSVSourceLoadWithBaseTime(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "cache.csv")

	content := `data_provider,stream_id,cron_schedule,from,include_children,base_time
0x0000000000000000000000000000000000000123,stream-one,0 0 * * *,1700000000,false,42
`
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp csv: %v", err)
	}

	source := NewCSVSource(filePath, "", log.DiscardLogger)
	specs, err := source.Load(context.Background(), map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error loading csv: %v", err)
	}

	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}

	spec := specs[0]
	if spec.BaseTime == nil {
		t.Fatalf("expected base_time to be parsed, got nil")
	}
	if *spec.BaseTime != 42 {
		t.Fatalf("expected base_time 42, got %d", *spec.BaseTime)
	}
}

func TestCSVSourceLoadWithBaseTimeNoIncludeChildren(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "cache.csv")

	content := `data_provider,stream_id,cron_schedule,base_time
0x0000000000000000000000000000000000000123,stream-one,0 0 * * *,314
`
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp csv: %v", err)
	}

	source := NewCSVSource(filePath, "", log.DiscardLogger)
	specs, err := source.Load(context.Background(), map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error loading csv: %v", err)
	}

	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}

	spec := specs[0]
	if spec.IncludeChildren {
		t.Fatalf("expected include_children to default to false")
	}
	if spec.BaseTime == nil || *spec.BaseTime != 314 {
		t.Fatalf("expected base_time 314, got %v", spec.BaseTime)
	}
}

func TestCSVSourceLoadWithInvalidBaseTime(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "cache.csv")

	content := `data_provider,stream_id,cron_schedule,from,include_children,base_time
0x0000000000000000000000000000000000000123,stream-one,0 0 * * *,1700000000,false,not-a-number
`
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp csv: %v", err)
	}

	source := NewCSVSource(filePath, "", log.DiscardLogger)
	_, err := source.Load(context.Background(), map[string]string{})
	if err == nil {
		t.Fatalf("expected error when parsing invalid base_time")
	}
}
