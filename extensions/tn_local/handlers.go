package tn_local

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
)

// pgUniqueViolation is the PostgreSQL error code for unique_violation.
const pgUniqueViolation = "23505"

var ethAddrRegex = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// validateStreamID checks that stream_id is 32 chars and starts with "st".
func validateStreamID(streamID string) error {
	if len(streamID) != 32 {
		return fmt.Errorf("stream_id must be exactly 32 characters, got %d", len(streamID))
	}
	if !strings.HasPrefix(streamID, "st") {
		return fmt.Errorf("stream_id must start with 'st'")
	}
	return nil
}

// validateStreamType checks that stream_type is "primitive" or "composed".
func validateStreamType(streamType string) error {
	if streamType != "primitive" && streamType != "composed" {
		return fmt.Errorf("stream_type must be 'primitive' or 'composed', got %q", streamType)
	}
	return nil
}

// validateDataProvider checks that data_provider is a valid Ethereum address (0x + 40 hex).
func validateDataProvider(dataProvider string) error {
	if !ethAddrRegex.MatchString(dataProvider) {
		return fmt.Errorf("data_provider must be a valid Ethereum address (0x + 40 hex chars)")
	}
	return nil
}

// isDuplicateKeyError checks if err is a PostgreSQL unique constraint violation (23505).
func isDuplicateKeyError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}
	// Fallback for non-pgx drivers (e.g. mocks): case-insensitive string match.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") || strings.Contains(msg, "unique constraint")
}

// CreateStream creates a local stream.
func (ext *Extension) CreateStream(ctx context.Context, req *CreateStreamRequest) (*CreateStreamResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if err := validateDataProvider(req.DataProvider); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if err := validateStreamType(req.StreamType); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}

	if err := ext.dbCreateStream(ctx, req.DataProvider, req.StreamID, req.StreamType); err != nil {
		if isDuplicateKeyError(err) {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream already exists: %s/%s", req.DataProvider, req.StreamID), nil)
		}
		ext.logger.Error("failed to create local stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to create stream", nil)
	}

	return &CreateStreamResponse{}, nil
}

// InsertRecords inserts records into a local primitive stream. (Task 4)
func (ext *Extension) InsertRecords(ctx context.Context, req *InsertRecordsRequest) (*InsertRecordsResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}

// InsertTaxonomy adds a taxonomy entry to a local composed stream. (Task 5)
func (ext *Extension) InsertTaxonomy(ctx context.Context, req *InsertTaxonomyRequest) (*InsertTaxonomyResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}

// GetRecord queries records from a local primitive stream. (Task 6)
func (ext *Extension) GetRecord(ctx context.Context, req *GetRecordRequest) (*GetRecordResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}

// GetIndex queries computed index values from a local stream. (Task 6)
func (ext *Extension) GetIndex(ctx context.Context, req *GetIndexRequest) (*GetIndexResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}

// ListStreams lists all local streams. (Task 6)
func (ext *Extension) ListStreams(ctx context.Context, req *ListStreamsRequest) (*ListStreamsResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}
