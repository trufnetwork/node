package internal

import "testing"

func TestParseDigestResultFromTxLog_HasMoreTrue(t *testing.T) {
	log := "INFO something\nNOTICE: auto_digest:{\"processed_days\":2,\"total_deleted_rows\":500,\"has_more_to_delete\":true}\nother"
	res, err := parseDigestResultFromTxLog(log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ProcessedDays != 2 {
		t.Fatalf("processed_days: want 2, got %d", res.ProcessedDays)
	}
	if res.TotalDeletedRows != 500 {
		t.Fatalf("total_deleted_rows: want 500, got %d", res.TotalDeletedRows)
	}
	if !res.HasMoreToDelete {
		t.Fatalf("has_more_to_delete: want true, got false")
	}
}

func TestParseDigestResultFromTxLog_HasMoreFalse(t *testing.T) {
	log := "auto_digest:{\"processed_days\":1,\"total_deleted_rows\":1234,\"has_more_to_delete\":false}"
	res, err := parseDigestResultFromTxLog(log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ProcessedDays != 1 {
		t.Fatalf("processed_days: want 1, got %d", res.ProcessedDays)
	}
	if res.TotalDeletedRows != 1234 {
		t.Fatalf("total_deleted_rows: want 1234, got %d", res.TotalDeletedRows)
	}
	if res.HasMoreToDelete {
		t.Fatalf("has_more_to_delete: want false, got true")
	}
}

func TestParseDigestResultFromTxLog_NoEntry(t *testing.T) {
	log := "INFO: nothing relevant here\nNOTICE: something else"
	_, err := parseDigestResultFromTxLog(log)
	if err == nil {
		t.Fatalf("expected error for missing auto_digest entry, got nil")
	}
}
