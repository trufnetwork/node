package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

// Operation represents a traceable operation with its name and common attributes
type Operation string

// Cache operation constants - define all traced operations in one place
const (
	// Entry point operations (precompile handlers)
	OpCacheCheck Operation = "cache.check" // has_cached_data precompile
	OpCacheGet   Operation = "cache.get"   // get_cached_data and get_cached_index_data precompile

	// Scheduler operations
	OpSchedulerJob     Operation = "scheduler.job"     // Cron job execution
	OpSchedulerRefresh Operation = "scheduler.refresh" // Stream refresh operation

	// Database operations
	OpDBCacheEvents   Operation = "db.cache_events"    // Store events in cache
	OpDBGetEvents     Operation = "db.get_events"      // Retrieve cached events
	OpDBHasCachedData Operation = "db.has_cached_data" // Check cache existence

	// TrufNetwork API operations
	OpTNListStreams        Operation = "tn.list_streams"         // List all streams
	OpTNGetCategoryStreams Operation = "tn.get_category_streams" // Get child streams
	OpTNGetRecordComposed  Operation = "tn.get_record_composed"  // Get composed records

	// Stream operations
	OpRefreshStream Operation = "refresh.stream" // Refresh single stream data
)

// OperationInfo provides metadata about each operation
type OperationInfo struct {
	Name        Operation
	Description string
	Required    []string // Required attribute keys
	Optional    []string // Optional attribute keys
}

// Registry of all operations with their metadata
var Operations = map[Operation]OperationInfo{
	OpCacheCheck: {
		Name:        OpCacheCheck,
		Description: "Check if cached data exists for a stream",
		Required:    []string{"provider", "stream", "from"},
		Optional:    []string{"to"},
	},
	OpCacheGet: {
		Name:        OpCacheGet,
		Description: "Retrieve cached data for a stream",
		Required:    []string{"provider", "stream", "from"},
		Optional:    []string{"to"},
	},
	OpSchedulerJob: {
		Name:        OpSchedulerJob,
		Description: "Execute scheduled refresh job",
		Required:    []string{"schedule"},
		Optional:    []string{},
	},
	OpSchedulerRefresh: {
		Name:        OpSchedulerRefresh,
		Description: "Refresh stream data from source",
		Required:    []string{"provider", "stream"},
		Optional:    []string{"type"},
	},
	OpDBCacheEvents: {
		Name:        OpDBCacheEvents,
		Description: "Store events in cache database",
		Required:    []string{"provider", "stream", "count"},
		Optional:    []string{},
	},
	OpDBGetEvents: {
		Name:        OpDBGetEvents,
		Description: "Retrieve events from cache database",
		Required:    []string{"provider", "stream", "from", "to"},
		Optional:    []string{},
	},
	OpDBHasCachedData: {
		Name:        OpDBHasCachedData,
		Description: "Check if cache has data for time range",
		Required:    []string{"provider", "stream", "from", "to"},
		Optional:    []string{},
	},
	OpTNListStreams: {
		Name:        OpTNListStreams,
		Description: "List streams from TrufNetwork",
		Required:    []string{"action"},
		Optional:    []string{},
	},
	OpTNGetCategoryStreams: {
		Name:        OpTNGetCategoryStreams,
		Description: "Get category streams from TrufNetwork",
		Required:    []string{"action"},
		Optional:    []string{},
	},
	OpTNGetRecordComposed: {
		Name:        OpTNGetRecordComposed,
		Description: "Get composed records from TrufNetwork",
		Required:    []string{"action"},
		Optional:    []string{},
	},
	OpRefreshStream: {
		Name:        OpRefreshStream,
		Description: "Refresh stream data from TrufNetwork",
		Required:    []string{"provider", "stream"},
		Optional:    []string{"type"},
	},
}

// StreamOperation creates a traced operation for stream-related activities
func StreamOperation(ctx context.Context, op Operation, provider, streamID string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	// Always include provider and stream
	baseAttrs := []attribute.KeyValue{
		attribute.String("provider", provider),
		attribute.String("stream", streamID),
	}
	return TraceOp(ctx, string(op), append(baseAttrs, attrs...)...)
}

// DatabaseOperation creates a traced operation for database activities
func DatabaseOperation(ctx context.Context, op Operation, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return TraceOp(ctx, string(op), attrs...)
}

// SchedulerOperation creates a traced operation for scheduler activities
func SchedulerOperation(ctx context.Context, op Operation, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return TraceOp(ctx, string(op), attrs...)
}

// TNOperation creates a traced operation for TrufNetwork API calls
func TNOperation(ctx context.Context, op Operation, action string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	// Always include the action name
	baseAttrs := []attribute.KeyValue{
		attribute.String("action", action),
	}
	return TraceOp(ctx, string(op), append(baseAttrs, attrs...)...)
}

// ValidateOperation checks if an operation is registered and valid
func ValidateOperation(op Operation) error {
	if _, exists := Operations[op]; !exists {
		return fmt.Errorf("unknown operation: %s", op)
	}
	return nil
}
