package sources

import (
	"context"
	"testing"
)

func TestInlineSourceLoadWithBaseTime(t *testing.T) {
	jsonConfig := `[{"data_provider":"0x0000000000000000000000000000000000000123","stream_id":"stream-one","cron_schedule":"0 0 * * *","base_time":128}]`

	source := NewInlineSource(jsonConfig)
	specs, err := source.Load(context.Background(), map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error loading inline config: %v", err)
	}

	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}

	if specs[0].BaseTime == nil || *specs[0].BaseTime != 128 {
		t.Fatalf("expected base_time 128, got %v", specs[0].BaseTime)
	}
}

func TestInlineSourceLoadInvalidBaseTime(t *testing.T) {
	jsonConfig := `[{"data_provider":"0x0000000000000000000000000000000000000123","stream_id":"stream-one","cron_schedule":"0 0 * * *","base_time":"invalid"}]`

	source := NewInlineSource(jsonConfig)
	if _, err := source.Load(context.Background(), map[string]string{}); err == nil {
		t.Fatalf("expected error when parsing inline base_time")
	}
}
