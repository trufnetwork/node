package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
)

const (
	// ExtensionAgentName is the standard name for extension agents accessing TN data
	ExtensionAgentName = "extension_agent"
)

// TNOperator defines the interface for TN operations
type TNOperator interface {
	// ListComposedStreams returns all composed streams for a given provider
	ListComposedStreams(ctx context.Context, provider string) ([]string, error)

	// GetCategoryStreams returns child streams for a composed stream
	GetCategoryStreams(ctx context.Context, provider, streamID string, activeFrom int64) ([]CategoryStream, error)

	// GetRecordComposed fetches records from a composed stream
	GetRecordComposed(ctx context.Context, provider, streamID string, from, to *int64) ([]ComposedRecord, error)
}

// CategoryStream represents a child stream in a category
type CategoryStream struct {
	DataProvider string
	StreamID     string
}

// ComposedRecord represents a record from a composed stream
type ComposedRecord struct {
	EventTime int64
	Value     *types.Decimal
}

// TNOperations implements TNOperator interface
type TNOperations struct {
	engine    common.Engine
	db        sql.DB
	namespace string
	logger    log.Logger
}

// NewTNOperations creates a new TNOperations instance
func NewTNOperations(engine common.Engine, db sql.DB, namespace string, logger log.Logger) *TNOperations {
	return &TNOperations{
		engine:    engine,
		db:        db,
		namespace: namespace,
		logger:    logger.New("tn_ops"),
	}
}

// ListComposedStreams returns all composed streams for a given provider
func (t *TNOperations) ListComposedStreams(ctx context.Context, provider string) ([]string, error) {
	t.logger.Debug("listing composed streams", "provider", provider)

	var composedStreams []string

	// Create a minimal engine context for read-only operations
	engineCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 0, // Use height 0 for current state
			},
			// NOTE: Currently using OverrideAuthz to access all streams including private ones
			// See TestExtensionAgentPermissions for verification
			Signer: []byte(ExtensionAgentName),
			Caller: ExtensionAgentName,
			TxID:   fmt.Sprintf("ext_agent_list_%d", time.Now().UnixNano()),
		},
		OverrideAuthz: true, // Read-only operation, no auth needed
	}

	// Use the list_streams action to get all streams for the provider
	_, err := t.engine.Call(
		engineCtx,
		t.db,
		"", // Actions don't use namespace
		"list_streams",
		[]any{
			provider,    // data_provider
			5000,        // limit (maximum allowed)
			0,           // offset
			"stream_id", // order_by
			int64(0),    // block_height (0 to get all streams)
		},
		func(row *common.Row) error {
			if len(row.Values) >= 3 {
				// row.Values: [data_provider, stream_id, stream_type, created_at]
				if streamID, ok := row.Values[1].(string); ok {
					if streamType, ok := row.Values[2].(string); ok && streamType == "composed" {
						composedStreams = append(composedStreams, streamID)
					}
				}
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("query composed streams: %w", err)
	}

	t.logger.Debug("found composed streams",
		"provider", provider,
		"count", len(composedStreams))

	return composedStreams, nil
}

// GetCategoryStreams returns child streams for a composed stream
func (t *TNOperations) GetCategoryStreams(ctx context.Context, provider, streamID string, activeFrom int64) ([]CategoryStream, error) {
	t.logger.Debug("getting category streams",
		"provider", provider,
		"stream", streamID,
		"active_from", activeFrom)

	var categoryStreams []CategoryStream

	// Create engine context
	engineCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 0,
			},
			Signer: []byte(ExtensionAgentName),
			Caller: ExtensionAgentName,
			TxID:   fmt.Sprintf("ext_agent_category_%d", time.Now().UnixNano()),
		},
		OverrideAuthz: true,
	}

	// Use the get_category_streams action to get all child streams
	_, err := t.engine.Call(
		engineCtx,
		t.db,
		"", // Actions don't use namespace
		"get_category_streams",
		[]any{
			provider,   // data_provider
			streamID,   // stream_id
			activeFrom, // active_from
			nil,        // active_to (get all)
		},
		func(row *common.Row) error {
			if len(row.Values) >= 2 {
				// row.Values: [data_provider, stream_id]
				if childProvider, ok := row.Values[0].(string); ok {
					if childStreamID, ok := row.Values[1].(string); ok {
						categoryStreams = append(categoryStreams, CategoryStream{
							DataProvider: childProvider,
							StreamID:     childStreamID,
						})
					}
				}
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("query category streams: %w", err)
	}

	t.logger.Debug("found category streams",
		"provider", provider,
		"stream", streamID,
		"count", len(categoryStreams))

	return categoryStreams, nil
}

// GetRecordComposed fetches records from a composed stream
func (t *TNOperations) GetRecordComposed(ctx context.Context, provider, streamID string, from, to *int64) ([]ComposedRecord, error) {
	t.logger.Debug("getting composed records",
		"provider", provider,
		"stream", streamID,
		"from", from,
		"to", to)

	var records []ComposedRecord

	// Create engine context
	engineCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 0,
			},
			Signer: []byte(ExtensionAgentName),
			Caller: ExtensionAgentName,
			TxID:   fmt.Sprintf("ext_agent_record_%d", time.Now().UnixNano()),
		},
		OverrideAuthz: true,
	}

	// Use the get_record_composed action
	_, err := t.engine.Call(
		engineCtx,
		t.db,
		"", // Actions don't use namespace
		"get_record_composed",
		[]any{
			provider, // data_provider
			streamID, // stream_id
			from,     // from timestamp
			to,       // to timestamp
			nil,      // frozen_at (not applicable for cache refresh)
		},
		func(row *common.Row) error {
			if len(row.Values) >= 2 {
				// Parse event time
				eventTime, err := parseEventTime(row.Values[0])
				if err != nil {
					return fmt.Errorf("parse event_time: %w", err)
				}

				// Parse value
				value, err := parseEventValue(row.Values[1])
				if err != nil {
					return fmt.Errorf("parse value: %w", err)
				}

				records = append(records, ComposedRecord{
					EventTime: eventTime,
					Value:     value,
				})
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("query composed records: %w", err)
	}

	t.logger.Debug("fetched composed records",
		"provider", provider,
		"stream", streamID,
		"count", len(records))

	return records, nil
}

// parseEventTime converts various types to int64 timestamp
func parseEventTime(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case string:
		// Try to parse string as int64
		var timestamp int64
		if _, err := fmt.Sscanf(val, "%d", &timestamp); err == nil {
			return timestamp, nil
		}
		return 0, fmt.Errorf("invalid timestamp string: %v", val)
	default:
		return 0, fmt.Errorf("unsupported timestamp type: %T", v)
	}
}

// parseEventValue converts TN's return types to *types.Decimal
// TN actions return either *types.Decimal or string for decimal values
func parseEventValue(v interface{}) (*types.Decimal, error) {
	switch val := v.(type) {
	case *types.Decimal:
		if val == nil {
			return nil, fmt.Errorf("nil decimal value")
		}
		// Ensure decimal(36,18) precision
		if err := val.SetPrecisionAndScale(36, 18); err != nil {
			return nil, fmt.Errorf("set precision and scale: %w", err)
		}
		return val, nil
	case string:
		// Parse string directly as decimal(36,18)
		return types.ParseDecimalExplicit(val, 36, 18)
	default:
		return nil, fmt.Errorf("unsupported value type: %T (expected *types.Decimal or string)", v)
	}
}
