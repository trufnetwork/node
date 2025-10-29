// Package internal provides internal utilities for the TN cache extension.
//
// This file contains the engine operations abstraction layer that allows
// the cache extension to interact with TrufNetwork's data through the Kwil
// engine rather than direct database access.
//
// Why we need engine operations:
// 1. Access to Actions: The TrufNetwork schema defines actions (stored procedures)
//    like 'get_category_streams' and 'get_record_composed' that contain business
//    logic and permissions. These can only be called through the Kwil engine.
// 2. Permission Model: Actions enforce TrufNetwork's permission model, ensuring
//    that data access respects user roles and stream visibility rules.
// 3. Business Logic: Actions contain important business logic for composing
//    streams, calculating aggregates, and transforming data that would be
//    complex to replicate with direct SQL.
//
// When to use engine operations vs direct DB access:
// - Use engine operations when:
//   * Calling TrufNetwork-defined actions
//   * Need to respect the permission model
//   * Accessing composed/calculated data
// - Use direct DB access (CacheDB) when:
//   * Reading/writing to cache-specific tables
//   * Need better performance for bulk operations
//   * Working with extension-private data

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

// EngineOperator defines the interface for operations that must go through
// the Kwil engine to access TrufNetwork actions and respect permissions.
type EngineOperator interface {
	// ListComposedStreams returns all composed streams for a given provider
	ListComposedStreams(ctx context.Context, provider string) ([]string, error)

	// GetCategoryStreams returns child streams for a composed stream
	GetCategoryStreams(ctx context.Context, provider, streamID string, activeFrom int64) ([]CategoryStream, error)

	// GetRecordComposed fetches records from a composed stream
	GetRecordComposed(ctx context.Context, provider, streamID string, from, to *int64) ([]EventRecord, error)

	// GetIndexComposed fetches index events from a composed stream
	GetIndexComposed(ctx context.Context, provider, streamID string, from, to *int64) ([]EventRecord, error)
}

// CategoryStream represents a child stream in a category
type CategoryStream struct {
	DataProvider string
	StreamID     string
}

// ComposedRecord represents a record from a composed stream
type EventRecord struct {
	EventTime int64
	Value     *types.Decimal
}

// EngineOperations implements the EngineOperator interface by calling
// TrufNetwork actions through the Kwil engine. This ensures all data
// access respects the defined permission model and business logic.
type EngineOperations struct {
	engine    common.Engine
	db        sql.DB
	namespace string
	logger    log.Logger
}

// NewEngineOperations creates a new EngineOperations instance
func NewEngineOperations(engine common.Engine, db sql.DB, namespace string, logger log.Logger) *EngineOperations {
	return &EngineOperations{
		engine:    engine,
		db:        db,
		namespace: namespace,
		logger:    logger.New("tn_ops"),
	}
}

// SetTx sets the underlying database pool
func (e *EngineOperations) SetTx(tx sql.DB) {
	e.db = tx
}

// createEngineContext creates a standard engine context for extension operations
// createEngineContext creates an engine context for executing actions.
// It sets up the proper caller context as the extension agent.
func (t *EngineOperations) createEngineContext(ctx context.Context, operation string) *common.EngineContext {
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
// callWithTrace executes an action through the engine with proper tracing.
// It handles the engine call protocol and processes results row by row.
func (t *EngineOperations) callWithTrace(ctx context.Context, engineCtx *common.EngineContext, action string, args []any, processResult func(*common.Row) error) error {
	// Map action to appropriate operation
	var op tracing.Operation
	switch action {
	case "list_streams":
		op = tracing.OpTNListStreams
	case "get_category_streams":
		op = tracing.OpTNGetCategoryStreams
	case "get_record_composed":
		op = tracing.OpTNGetRecordComposed
	case "get_index_composed":
		op = tracing.OpTNGetIndexComposed
	default:
		// Fallback for unknown actions
		op = tracing.Operation("tn." + action)
	}

	// Use middleware for tracing
	_, err := tracing.TracedTNOperation(ctx, op, action,
		func(traceCtx context.Context) (any, error) {
			// Update engine context with traced context
			engineCtx.TxContext.Ctx = traceCtx

			// Call the engine
			_, err := t.engine.Call(engineCtx, t.db, t.namespace, action, args, processResult)
			return nil, err
		})

	return err
}

// ListComposedStreams returns all composed streams for a given provider
// ListComposedStreams calls the 'list_composed_streams' action through the engine.
// This action contains logic to identify which streams are composed (have child streams)
// rather than primitive data streams.
func (t *EngineOperations) ListComposedStreams(ctx context.Context, provider string) ([]string, error) {
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
// GetCategoryStreams calls the 'get_category_streams' action through the engine.
// This action returns the child streams that compose a parent stream, respecting
// the temporal validity (active_from) of the composition relationships.
func (t *EngineOperations) GetCategoryStreams(ctx context.Context, provider, streamID string, activeFrom int64) ([]CategoryStream, error) {
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
// GetRecordComposed calls the 'get_record_composed' action through the engine.
// This action handles the complex logic of aggregating data from child streams
// into composed values, including weighted averages and other calculations.
func (t *EngineOperations) GetRecordComposed(ctx context.Context, provider, streamID string, from, to *int64) ([]EventRecord, error) {
	t.logger.Debug("getting composed records",
		"provider", provider,
		"stream", streamID,
		"from", from,
		"to", to)

	// if from is nil, we should set to 0, meaning it's all available
	if from == nil {
		from = new(int64)
		*from = 0
	}

	var records []EventRecord

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
			// false,    // don't use cache to get new data
			// It's false by default. let's omit to be ok with new and old version of actions
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

				records = append(records, EventRecord{
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

// GetIndexComposed fetches index events from a composed stream
func (t *EngineOperations) GetIndexComposed(ctx context.Context, provider, streamID string, from, to *int64) ([]EventRecord, error) {
	t.logger.Debug("getting composed index records", "provider", provider, "stream", streamID)

	// if from is nil, we should set to 0, meaning it's all available
	if from == nil {
		from = new(int64)
		*from = 0
	}

	var indexEvents []EventRecord
	action := "get_index_composed"

	// Create engine context for this operation
	engineCtx := t.createEngineContext(ctx, action)

	// Build arguments for the action call
	args := []any{
		provider,
		streamID,
		from, // from timestamp
		to,   // to timestamp (fetch all available)
		nil,  // frozen_at (not applicable for cache refresh)
		nil,  // base_time (NULL to use default)
		// It's false by default. let's omit to be ok with new and old version of actions
		// false, // don't use cache to get new data
	}

	err := t.callWithTrace(
		ctx,
		engineCtx,
		action,
		args,
		func(row *common.Row) error {
			// Parse each row into a CachedIndexEvent
			if len(row.Values) >= 2 {
				// Parse event time
				eventTime, err := parsing.ParseEventTime(row.Values[0])
				if err != nil {
					return fmt.Errorf("parse event_time: %w", err)
				}

				// Parse the index value
				indexValue, err := parsing.ParseEventValue(row.Values[1])
				if err != nil {
					return fmt.Errorf("parse index value: %w", err)
				}

				indexEvents = append(indexEvents, EventRecord{
					EventTime: eventTime,
					Value:     indexValue,
				})
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("call action %s: %w", action, err)
	}

	t.logger.Debug("fetched index stream data",
		"action", action,
		"events", len(indexEvents),
		"provider", provider,
		"stream", streamID)

	return indexEvents, nil
}

// GetLatestMetadataInt retrieves the latest metadata integer value for the given stream and key.
func (t *EngineOperations) GetLatestMetadataInt(ctx context.Context, provider, streamID, key string) (*int64, error) {
	engineCtx := t.createEngineContext(ctx, "get_latest_metadata_int")

	var valuePtr *int64

	err := t.callWithTrace(
		ctx,
		engineCtx,
		"get_latest_metadata_int",
		[]any{provider, streamID, key},
		func(row *common.Row) error {
			if len(row.Values) == 0 || row.Values[0] == nil {
				return nil
			}

			switch v := row.Values[0].(type) {
			case int64:
				value := v
				valuePtr = &value
			case int32:
				value := int64(v)
				valuePtr = &value
			default:
				return fmt.Errorf("unexpected metadata int type %T", row.Values[0])
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("get_latest_metadata_int: %w", err)
	}

	return valuePtr, nil
}
