package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/parsing"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
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

// createEngineContext creates a standard engine context for extension operations
func (t *TNOperations) createEngineContext(ctx context.Context, operation string) *common.EngineContext {
	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 0, // Use height 0 for current state
			},
			Signer: []byte(ExtensionAgentName),
			Caller: ExtensionAgentName,
			TxID:   fmt.Sprintf("ext_agent_%s_%d", operation, time.Now().UnixNano()),
		},
		OverrideAuthz: true, // Read-only operations don't need auth
	}
}

// callWithTrace wraps engine calls with tracing
func (t *TNOperations) callWithTrace(ctx context.Context, engineCtx *common.EngineContext, action string, args []any, processResult func(*common.Row) error) (err error) {
	// Map action to appropriate operation
	var op tracing.Operation
	switch action {
	case "list_streams":
		op = tracing.OpTNListStreams
	case "get_category_streams":
		op = tracing.OpTNGetCategoryStreams
	case "get_record_composed":
		op = tracing.OpTNGetRecordComposed
	default:
		// Fallback for unknown actions
		op = tracing.Operation("tn." + action)
	}
	
	// Start span with action details
	spanCtx, end := tracing.TNOperation(ctx, op, action)
	defer func() {
		end(err)
	}()
	
	// Update engine context with traced context
	engineCtx.TxContext.Ctx = spanCtx
	
	// Call the engine
	_, err = t.engine.Call(engineCtx, t.db, "", action, args, processResult)
	return err
}

// ListComposedStreams returns all composed streams for a given provider
func (t *TNOperations) ListComposedStreams(ctx context.Context, provider string) ([]string, error) {
	t.logger.Debug("listing composed streams", "provider", provider)

	var composedStreams []string

	// Create engine context for this operation
	engineCtx := t.createEngineContext(ctx, "list_streams")

	// Use the list_streams action to get all streams for the provider
	err := t.callWithTrace(
		ctx,
		engineCtx,
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

	// Create engine context for this operation
	engineCtx := t.createEngineContext(ctx, "get_category_streams")

	// Use the get_category_streams action to get all child streams
	err := t.callWithTrace(
		ctx,
		engineCtx,
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

	// Create engine context for this operation
	engineCtx := t.createEngineContext(ctx, "get_record_composed")

	// Use the get_record_composed action
	err := t.callWithTrace(
		ctx,
		engineCtx,
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
				eventTime, err := parsing.ParseEventTime(row.Values[0])
				if err != nil {
					return fmt.Errorf("parse event_time: %w", err)
				}

				// Parse value
				value, err := parsing.ParseEventValue(row.Values[1])
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

