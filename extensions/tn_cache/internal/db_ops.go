// Package internal provides database operations for the TN Cache extension.
//
// IMPORTANT: Table Ownership Design
//
// The TN Cache extension is designed to be the EXCLUSIVE manager of the tables it creates:
// - _` + CacheSchemaName + `.cached_events
// - _` + CacheSchemaName + `.cached_streams
//
// No other system or extension should directly modify these tables. This design assumption
// allows us to:
// 1. Use simpler transaction isolation (READ COMMITTED instead of SERIALIZABLE)
// 2. Cache state in memory for performance without worrying about external changes
// 3. Make assumptions about data consistency without defensive checks
//
// If this assumption changes in the future, the following would need to be revisited:
// - Transaction isolation levels
// - Cache invalidation strategies
// - Concurrent modification handling
package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
)

// CacheDB handles database operations for the TRUF.NETWORK cache
type CacheDB struct {
	logger log.Logger
	db     sql.DB
}

// SetTx sets the underlying database pool
func (c *CacheDB) SetTx(tx sql.DB) {
	c.db = tx
}

// StreamCacheConfig represents a stream's caching configuration
type StreamCacheConfig struct {
	DataProvider  string
	StreamID      string
	FromTimestamp int64
	LastRefreshed int64
	CronSchedule  string
}

// CachedEvent represents a cached event from a stream
type CachedEvent struct {
	DataProvider string
	StreamID     string
	EventTime    int64
	Value        *types.Decimal // Use high-precision decimal for decimal(36,18) values
}

// NewCacheDB creates a new CacheDB instance with sql.DB
func NewCacheDB(db sql.DB, logger log.Logger) *CacheDB {
	return &CacheDB{
		logger: logger.New("tn_cache_db"),
		db:     db,
	}
}

// AddStreamConfig adds or updates a stream's cache configuration
func (c *CacheDB) AddStreamConfig(ctx context.Context, config StreamCacheConfig) error {
	c.logger.Debug("adding stream cache config",
		"data_provider", config.DataProvider,
		"stream_id", config.StreamID)

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert or update the stream config using UPSERT
	_, err = tx.Execute(ctx, `
		INSERT INTO `+constants.CacheSchemaName+`.cached_streams 
			(data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
		VALUES 
			($1, $2, $3, $4, $5)
		ON CONFLICT (data_provider, stream_id) 
		DO UPDATE SET
			from_timestamp = EXCLUDED.from_timestamp,
			last_refreshed = EXCLUDED.last_refreshed,
			cron_schedule = EXCLUDED.cron_schedule
	`, config.DataProvider, config.StreamID, config.FromTimestamp, config.LastRefreshed, config.CronSchedule)

	if err != nil {
		return fmt.Errorf("upsert stream config: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	c.logger.Debug("stream cache config added successfully")

	return nil
}

// AddStreamConfigs adds or updates multiple stream cache configurations in a single transaction
func (c *CacheDB) AddStreamConfigs(ctx context.Context, configs []StreamCacheConfig) error {
	if len(configs) == 0 {
		return nil // Nothing to do
	}

	c.logger.Debug("adding multiple stream cache configs", "count", len(configs))

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Build arrays for batch insert using UNNEST for efficiency
	var dataProviders, streamIDs, cronSchedules []string
	var fromTimestamps, lastRefresheds []int64

	for _, config := range configs {
		dataProviders = append(dataProviders, config.DataProvider)
		streamIDs = append(streamIDs, config.StreamID)
		fromTimestamps = append(fromTimestamps, config.FromTimestamp)
		lastRefresheds = append(lastRefresheds, config.LastRefreshed)
		cronSchedules = append(cronSchedules, config.CronSchedule)
	}

	// Execute the batch insert with UPSERT logic
	_, err = tx.Execute(ctx, `
		INSERT INTO `+constants.CacheSchemaName+`.cached_streams 
			(data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
		SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::INT8[], $4::INT8[], $5::TEXT[])
		ON CONFLICT (data_provider, stream_id) 
		DO UPDATE SET
			from_timestamp = EXCLUDED.from_timestamp,
			last_refreshed = EXCLUDED.last_refreshed,
			cron_schedule = EXCLUDED.cron_schedule
	`, dataProviders, streamIDs, fromTimestamps, lastRefresheds, cronSchedules)

	if err != nil {
		return fmt.Errorf("batch upsert stream configs: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	c.logger.Info("successfully added stream cache configs", "count", len(configs))
	return nil
}

// GetStreamConfig retrieves a stream's cache configuration
func (c *CacheDB) GetStreamConfig(ctx context.Context, dataProvider, streamID string) (*StreamCacheConfig, error) {
	c.logger.Debug("getting stream cache config",
		"data_provider", dataProvider,
		"stream_id", streamID)

	return c.getStreamConfigPool(ctx, dataProvider, streamID)
}

// ListStreamConfigs retrieves all stream cache configurations
func (c *CacheDB) ListStreamConfigs(ctx context.Context) ([]StreamCacheConfig, error) {
	c.logger.Debug("listing all stream cache configs")

	results, err := c.db.Execute(ctx, `
		SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule
		FROM `+constants.CacheSchemaName+`.cached_streams
		ORDER BY data_provider, stream_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query stream configs: %w", err)
	}

	var configs []StreamCacheConfig
	for _, row := range results.Rows {
		if len(row) != 5 {
			return nil, fmt.Errorf("expected 5 columns, got %d", len(row))
		}
		
		dataProvider, ok := row[0].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert data_provider to string")
		}
		
		streamID, ok := row[1].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert stream_id to string")
		}
		
		fromTimestamp, ok := row[2].(int64)
		if !ok {
			return nil, fmt.Errorf("failed to convert from_timestamp to int64")
		}
		
		lastRefreshed, ok := row[3].(int64)
		if !ok {
			return nil, fmt.Errorf("failed to convert last_refreshed to int64")
		}
		
		cronSchedule, ok := row[4].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cron_schedule to string")
		}
		
		config := StreamCacheConfig{
			DataProvider:  dataProvider,
			StreamID:      streamID,
			FromTimestamp: fromTimestamp,
			LastRefreshed: lastRefreshed,
			CronSchedule:  cronSchedule,
		}
		configs = append(configs, config)
	}

	return configs, nil
}

// CacheEvents stores events in the cache
func (c *CacheDB) CacheEvents(ctx context.Context, events []CachedEvent) (err error) {
	if len(events) == 0 {
		return nil
	}

	// Add tracing
	ctx, end := tracing.StreamOperation(ctx, tracing.OpDBCacheEvents, events[0].DataProvider, events[0].StreamID,
		attribute.Int("count", len(events)))
	defer func() {
		end(err)
	}()

	c.logger.Debug("caching events",
		"data_provider", events[0].DataProvider,
		"stream_id", events[0].StreamID,
		"count", len(events))

	// Use READ COMMITTED isolation for event caching (default is fine)
	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Insert events in batches using UNNEST for efficiency
	// PostgreSQL has a limit of 65535 parameters, and we use 4 per event
	// Use 10000 as batch size for safety (40000 parameters)
	batchSize := 10000
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]

		// Build arrays for UNNEST
		var dataProviders, streamIDs []string
		var eventTimes []int64
		var values []*types.Decimal

		for _, event := range batch {
			dataProviders = append(dataProviders, event.DataProvider)
			streamIDs = append(streamIDs, event.StreamID)
			eventTimes = append(eventTimes, event.EventTime)
			values = append(values, event.Value)
		}

		// Use UNNEST for efficient batch insert
		_, err = tx.Execute(ctx, `
			INSERT INTO `+constants.CacheSchemaName+`.cached_events 
				(data_provider, stream_id, event_time, value)
			SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::INT8[], $4::DECIMAL(36,18)[])
			ON CONFLICT (data_provider, stream_id, event_time) 
			DO UPDATE SET
				value = EXCLUDED.value
		`, dataProviders, streamIDs, eventTimes, values)

		if err != nil {
			return fmt.Errorf("insert events batch: %w", err)
		}
	}

	// Finally, update the last refreshed timestamp for the stream
	if len(events) > 0 {
		event := events[0]
		now := time.Now().Unix()
		_, err = tx.Execute(ctx, `
			UPDATE `+constants.CacheSchemaName+`.cached_streams
			SET last_refreshed = $3
			WHERE data_provider = $1 AND stream_id = $2
		`, event.DataProvider, event.StreamID, now)

		if err != nil {
			return fmt.Errorf("update last refreshed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// CacheEventsWithIndex stores both raw events and index events atomically in the cache
func (c *CacheDB) CacheEventsWithIndex(ctx context.Context, events []CachedEvent, indexEvents []CachedEvent) (err error) {
	// If neither have events, nothing to do
	if len(events) == 0 && len(indexEvents) == 0 {
		return nil
	}

	// Determine primary stream for tracing
	var primaryProvider, primaryStreamID string
	if len(events) > 0 {
		primaryProvider = events[0].DataProvider
		primaryStreamID = events[0].StreamID
	} else if len(indexEvents) > 0 {
		primaryProvider = indexEvents[0].DataProvider
		primaryStreamID = indexEvents[0].StreamID
	}

	// Add tracing
	ctx, end := tracing.StreamOperation(ctx, tracing.OpDBCacheEvents, primaryProvider, primaryStreamID,
		attribute.Int("raw_count", len(events)),
		attribute.Int("index_count", len(indexEvents)))
	defer func() {
		end(err)
	}()

	c.logger.Debug("caching events with index",
		"data_provider", primaryProvider,
		"stream_id", primaryStreamID,
		"raw_count", len(events),
		"index_count", len(indexEvents))

	// Begin transaction for atomic updates
	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Cache raw events if provided
	if len(events) > 0 {
		// Insert events in batches using UNNEST for efficiency
		batchSize := 10000
		for i := 0; i < len(events); i += batchSize {
			end := i + batchSize
			if end > len(events) {
				end = len(events)
			}

			batch := events[i:end]

			// Build arrays for UNNEST
			var dataProviders, streamIDs []string
			var eventTimes []int64
			var values []*types.Decimal

			for _, event := range batch {
				dataProviders = append(dataProviders, event.DataProvider)
				streamIDs = append(streamIDs, event.StreamID)
				eventTimes = append(eventTimes, event.EventTime)
				values = append(values, event.Value)
			}

			// Use UNNEST for efficient batch insert
			_, err = tx.Execute(ctx, `
				INSERT INTO `+constants.CacheSchemaName+`.cached_events 
					(data_provider, stream_id, event_time, value)
				SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::INT8[], $4::DECIMAL(36,18)[])
				ON CONFLICT (data_provider, stream_id, event_time) 
				DO UPDATE SET
					value = EXCLUDED.value
			`, dataProviders, streamIDs, eventTimes, values)

			if err != nil {
				return fmt.Errorf("insert events batch: %w", err)
			}
		}
	}

	// Cache index events if provided
	if len(indexEvents) > 0 {
		// Insert index events in batches
		batchSize := 10000
		for i := 0; i < len(indexEvents); i += batchSize {
			end := i + batchSize
			if end > len(indexEvents) {
				end = len(indexEvents)
			}

			batch := indexEvents[i:end]

			// Build arrays for UNNEST
			var dataProviders, streamIDs []string
			var eventTimes []int64
			var indexValues []*types.Decimal

			for _, event := range batch {
				dataProviders = append(dataProviders, event.DataProvider)
				streamIDs = append(streamIDs, event.StreamID)
				eventTimes = append(eventTimes, event.EventTime)
				indexValues = append(indexValues, event.Value)
			}

			// Use UNNEST for efficient batch insert
			_, err = tx.Execute(ctx, `
				INSERT INTO `+constants.CacheSchemaName+`.cached_index_events 
					(data_provider, stream_id, event_time, value)
				SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::INT8[], $4::DECIMAL(36,18)[])
				ON CONFLICT (data_provider, stream_id, event_time) 
				DO UPDATE SET
					value = EXCLUDED.value
			`, dataProviders, streamIDs, eventTimes, indexValues)

			if err != nil {
				return fmt.Errorf("insert index events batch: %w", err)
			}
		}
	}

	// Update the last refreshed timestamp for the stream
	if primaryProvider != "" && primaryStreamID != "" {
		now := time.Now().Unix()
		_, err = tx.Execute(ctx, `
			UPDATE `+constants.CacheSchemaName+`.cached_streams
			SET last_refreshed = $3
			WHERE data_provider = $1 AND stream_id = $2
		`, primaryProvider, primaryStreamID, now)

		if err != nil {
			return fmt.Errorf("update last refreshed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// GetCachedEvents retrieves events from the cache for a specific time range
func (c *CacheDB) GetCachedEvents(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) (events []CachedEvent, err error) {
	// Add tracing
	ctx, end := tracing.StreamOperation(ctx, tracing.OpDBGetEvents, dataProvider, streamID,
		attribute.Int64("from", fromTime),
		attribute.Int64("to", toTime))
	defer func() {
		end(err)
	}()

	c.logger.Debug("getting cached events",
		"data_provider", dataProvider,
		"stream_id", streamID,
		"from_time", fromTime,
		"to_time", toTime)

	var results *sql.ResultSet
	if toTime > 0 {
		results, err = c.db.Execute(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3 AND event_time <= $4
			ORDER BY event_time
		`, dataProvider, streamID, fromTime, toTime)
	} else {
		results, err = c.db.Execute(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3
			ORDER BY event_time
		`, dataProvider, streamID, fromTime)
	}

	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}

	events = make([]CachedEvent, 0, len(results.Rows))
	for _, row := range results.Rows {
		if len(row) != 4 {
			return nil, fmt.Errorf("expected 4 columns, got %d", len(row))
		}
		
		dataProvider, ok := row[0].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert data_provider to string")
		}
		
		streamID, ok := row[1].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert stream_id to string")
		}
		
		eventTime, ok := row[2].(int64)
		if !ok {
			return nil, fmt.Errorf("failed to convert event_time to int64")
		}
		
		value, ok := row[3].(*types.Decimal)
		if !ok {
			return nil, fmt.Errorf("failed to convert value to *types.Decimal")
		}
		
		event := CachedEvent{
			DataProvider: dataProvider,
			StreamID:     streamID,
			EventTime:    eventTime,
			Value:        value,
		}
		events = append(events, event)
	}

	return events, nil
}

// GetCachedIndex retrieves index events from the cache for a specific time range
func (c *CacheDB) GetCachedIndex(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) (events []CachedEvent, err error) {
	// Add tracing
	ctx, end := tracing.StreamOperation(ctx, tracing.OpDBGetEvents, dataProvider, streamID,
		attribute.Int64("from", fromTime),
		attribute.Int64("to", toTime),
		attribute.String("type", "index"))
	defer func() {
		end(err)
	}()

	c.logger.Debug("getting cached index events",
		"data_provider", dataProvider,
		"stream_id", streamID,
		"from_time", fromTime,
		"to_time", toTime)

	var results *sql.ResultSet

	// Special case: latest value only (both fromTime and toTime are 0)
	if fromTime == 0 && toTime == 0 {
		results, err = c.db.Execute(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_index_events
			WHERE data_provider = $1 AND stream_id = $2
			ORDER BY event_time DESC
			LIMIT 1
		`, dataProvider, streamID)
	} else if toTime > 0 {
		results, err = c.db.Execute(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_index_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3 AND event_time <= $4
			ORDER BY event_time
		`, dataProvider, streamID, fromTime, toTime)
	} else {
		results, err = c.db.Execute(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_index_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3
			ORDER BY event_time
		`, dataProvider, streamID, fromTime)
	}

	if err != nil {
		return nil, fmt.Errorf("query index events: %w", err)
	}

	events = make([]CachedEvent, 0, len(results.Rows))
	for _, row := range results.Rows {
		if len(row) != 4 {
			return nil, fmt.Errorf("expected 4 columns, got %d", len(row))
		}
		
		dataProvider, ok := row[0].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert data_provider to string")
		}
		
		streamID, ok := row[1].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert stream_id to string")
		}
		
		eventTime, ok := row[2].(int64)
		if !ok {
			return nil, fmt.Errorf("failed to convert event_time to int64")
		}
		
		value, ok := row[3].(*types.Decimal)
		if !ok {
			return nil, fmt.Errorf("failed to convert value to *types.Decimal")
		}
		
		event := CachedEvent{
			DataProvider: dataProvider,
			StreamID:     streamID,
			EventTime:    eventTime,
			Value:        value,
		}
		events = append(events, event)
	}

	return events, nil
}

// DeleteStreamData deletes all cached events for a stream
func (c *CacheDB) DeleteStreamData(ctx context.Context, dataProvider, streamID string) error {
	c.logger.Debug("deleting stream data",
		"data_provider", dataProvider,
		"stream_id", streamID)

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Delete events for the stream
	_, err = tx.Execute(ctx, `
		DELETE FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return fmt.Errorf("delete stream events: %w", err)
	}

	// Delete index events for the stream
	_, err = tx.Execute(ctx, `
		DELETE FROM `+constants.CacheSchemaName+`.cached_index_events
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return fmt.Errorf("delete stream index events: %w", err)
	}

	// Delete stream configuration
	_, err = tx.Execute(ctx, `
		DELETE FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return fmt.Errorf("delete stream config: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// CleanupCache deletes all cache data
func (c *CacheDB) CleanupCache(ctx context.Context) error {
	c.logger.Debug("cleaning up entire cache")

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Delete all events
	_, err = tx.Execute(ctx, `DELETE FROM `+constants.CacheSchemaName+`.cached_events`)
	if err != nil {
		return fmt.Errorf("delete all events: %w", err)
	}

	// Delete all index events
	_, err = tx.Execute(ctx, `DELETE FROM `+constants.CacheSchemaName+`.cached_index_events`)
	if err != nil {
		return fmt.Errorf("delete all index events: %w", err)
	}

	// Delete all stream configurations
	_, err = tx.Execute(ctx, `DELETE FROM `+constants.CacheSchemaName+`.cached_streams`)
	if err != nil {
		return fmt.Errorf("delete all stream configs: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// HasCachedData checks if there is cached data for a stream in a time range
func (c *CacheDB) HasCachedData(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) (hasData bool, err error) {
	// Add tracing
	ctx, end := tracing.StreamOperation(ctx, tracing.OpDBHasCachedData, dataProvider, streamID,
		attribute.Int64("from", fromTime),
		attribute.Int64("to", toTime))
	defer func() {
		end(err)
	}()

	c.logger.Debug("checking for cached data",
		"data_provider", dataProvider,
		"stream_id", streamID,
		"from_time", fromTime,
		"to_time", toTime)

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// First, check if the stream is configured for caching
	results, err := tx.Execute(ctx, `
		SELECT COUNT(*) > 0
		FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
			AND (from_timestamp IS NULL OR from_timestamp <= $3)
	`, dataProvider, streamID, fromTime)

	if err != nil {
		return false, fmt.Errorf("check stream config: %w", err)
	}

	if len(results.Rows) == 0 || len(results.Rows[0]) != 1 {
		return false, fmt.Errorf("unexpected result structure for stream exists check")
	}

	streamExists, ok := results.Rows[0][0].(bool)
	if !ok {
		return false, fmt.Errorf("failed to convert stream exists result to bool")
	}

	if !streamExists {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit transaction: %w", err)
		}
		return false, nil
	}

	// Then, check if there are events in the cache
	var eventCount int64
	if toTime == 0 {
		// No upper bound specified - to is treated as max_int8 (end of time)
		results, err := tx.Execute(ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		`, dataProvider, streamID, fromTime)
		if err != nil {
			return false, fmt.Errorf("failed to count cached events: %w", err)
		}

		if len(results.Rows) == 0 || len(results.Rows[0]) != 1 {
			return false, fmt.Errorf("unexpected result structure for event count")
		}

		eventCount, ok = results.Rows[0][0].(int64)
		if !ok {
			return false, fmt.Errorf("failed to convert event count to int64")
		}
	} else {
		// Upper bound specified
		results, err := tx.Execute(ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3 AND event_time <= $4
		`, dataProvider, streamID, fromTime, toTime)
		if err != nil {
			return false, fmt.Errorf("failed to count cached events: %w", err)
		}

		if len(results.Rows) == 0 || len(results.Rows[0]) != 1 {
			return false, fmt.Errorf("unexpected result structure for event count")
		}

		eventCount, ok = results.Rows[0][0].(int64)
		if !ok {
			return false, fmt.Errorf("failed to convert event count to int64")
		}
	}

	hasData = eventCount > 0

	// If no direct events found, check for an anchor record (last event before fromTime)
	if !hasData && fromTime != 0 {
		anchorResults, err := tx.Execute(ctx, `
			SELECT 1 FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time < $3
			LIMIT 1
		`, dataProvider, streamID, fromTime)
		if err != nil {
			return false, fmt.Errorf("failed to query anchor record: %w", err)
		}
		hasData = len(anchorResults.Rows) > 0
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit transaction: %w", err)
	}

	return hasData, nil
}

// UpdateStreamConfigsAtomic atomically updates the cached_streams table
// It adds/updates configs in newConfigs and deletes configs in toDelete
func (c *CacheDB) UpdateStreamConfigsAtomic(ctx context.Context, newConfigs []StreamCacheConfig, toDelete []StreamCacheConfig) error {
	c.logger.Debug("atomic stream config update",
		"new_count", len(newConfigs),
		"delete_count", len(toDelete))

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// First, delete removed streams using batch operation
	if len(toDelete) > 0 {
		var deleteProviders, deleteStreamIDs []string
		for _, config := range toDelete {
			deleteProviders = append(deleteProviders, config.DataProvider)
			deleteStreamIDs = append(deleteStreamIDs, config.StreamID)
		}

		// Batch delete using array operations
		_, err = tx.Execute(ctx, `
			DELETE FROM `+constants.CacheSchemaName+`.cached_streams
			WHERE (data_provider, stream_id) IN (
				SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[])
			)
		`, deleteProviders, deleteStreamIDs)
		if err != nil {
			return fmt.Errorf("batch delete stream configs: %w", err)
		}
	}

	// Then, add/update new streams using batch operation
	if len(newConfigs) > 0 {
		var dataProviders, streamIDs, cronSchedules []string
		var fromTimestamps, lastRefresheds []int64

		for _, config := range newConfigs {
			dataProviders = append(dataProviders, config.DataProvider)
			streamIDs = append(streamIDs, config.StreamID)
			fromTimestamps = append(fromTimestamps, config.FromTimestamp)
			lastRefresheds = append(lastRefresheds, config.LastRefreshed)
			cronSchedules = append(cronSchedules, config.CronSchedule)
		}

		// Execute the batch upsert, preserving last_refreshed if it exists
		_, err = tx.Execute(ctx, `
			INSERT INTO `+constants.CacheSchemaName+`.cached_streams 
				(data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
			SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::INT8[], $4::INT8[], $5::TEXT[])
			ON CONFLICT (data_provider, stream_id) 
			DO UPDATE SET
				from_timestamp = EXCLUDED.from_timestamp,
				last_refreshed = COALESCE(NULLIF(EXCLUDED.last_refreshed, 0), cached_streams.last_refreshed),
				cron_schedule = EXCLUDED.cron_schedule
		`, dataProviders, streamIDs, fromTimestamps, lastRefresheds, cronSchedules)

		if err != nil {
			return fmt.Errorf("batch upsert stream configs: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	c.logger.Info("atomic stream config update completed",
		"added/updated", len(newConfigs),
		"deleted", len(toDelete))

	return nil
}

// getStreamConfigPool retrieves a stream's cache configuration using pool
func (c *CacheDB) getStreamConfigPool(ctx context.Context, dataProvider, streamID string) (*StreamCacheConfig, error) {
	results, err := c.db.Execute(ctx, `
		SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule
		FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return nil, fmt.Errorf("query stream config: %w", err)
	}

	if len(results.Rows) == 0 {
		return nil, sql.ErrNoRows
	}

	if len(results.Rows) != 1 {
		return nil, fmt.Errorf("expected 1 row, got %d", len(results.Rows))
	}

	row := results.Rows[0]
	if len(row) != 5 {
		return nil, fmt.Errorf("expected 5 columns, got %d", len(row))
	}

	dataProviderResult, ok := row[0].(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert data_provider to string")
	}

	streamIDResult, ok := row[1].(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert stream_id to string")
	}

	fromTimestamp, ok := row[2].(int64)
	if !ok {
		return nil, fmt.Errorf("failed to convert from_timestamp to int64")
	}

	lastRefreshed, ok := row[3].(int64)
	if !ok {
		return nil, fmt.Errorf("failed to convert last_refreshed to int64")
	}

	cronSchedule, ok := row[4].(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert cron_schedule to string")
	}

	config := &StreamCacheConfig{
		DataProvider:  dataProviderResult,
		StreamID:      streamIDResult,
		FromTimestamp: fromTimestamp,
		LastRefreshed: lastRefreshed,
		CronSchedule:  cronSchedule,
	}

	return config, nil
}

// StreamCountInfo holds stream information with event counts
type StreamCountInfo struct {
	DataProvider string
	StreamID     string
	EventCount   int64
}

// QueryCachedStreamsWithCounts returns all cached streams with their event counts
func (c *CacheDB) QueryCachedStreamsWithCounts(ctx context.Context) ([]StreamCountInfo, error) {
	query := `
		SELECT cs.data_provider, cs.stream_id, COALESCE(ce.event_count, 0) as event_count
		FROM ` + constants.CacheSchemaName + `.cached_streams cs
		LEFT JOIN (
			SELECT data_provider, stream_id, COUNT(*) as event_count
			FROM ` + constants.CacheSchemaName + `.cached_events
			GROUP BY data_provider, stream_id
		) ce ON cs.data_provider = ce.data_provider AND cs.stream_id = ce.stream_id
		ORDER BY cs.data_provider, cs.stream_id
	`

	results, err := c.db.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query cached streams with counts: %w", err)
	}

	var streamInfos []StreamCountInfo
	for _, row := range results.Rows {
		if len(row) != 3 {
			return nil, fmt.Errorf("expected 3 columns, got %d", len(row))
		}
		
		dataProvider, ok := row[0].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert data_provider to string")
		}
		
		streamID, ok := row[1].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert stream_id to string")
		}
		
		eventCount, ok := row[2].(int64)
		if !ok {
			return nil, fmt.Errorf("failed to convert event_count to int64")
		}
		
		info := StreamCountInfo{
			DataProvider: dataProvider,
			StreamID:     streamID,
			EventCount:   eventCount,
		}
		streamInfos = append(streamInfos, info)
	}

	return streamInfos, nil
}

// SetupCacheSchema creates the necessary database schema for the cache
func (c *CacheDB) SetupCacheSchema(ctx context.Context) error {
	c.logger.Info("setting up cache schema")

	// Begin a transaction to ensure atomicity of schema creation
	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create schema - private schema not prefixed with ds_, ignored by consensus
	if _, err := tx.Execute(ctx, `CREATE SCHEMA IF NOT EXISTS `+constants.CacheSchemaName); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Create cached_streams table
	if _, err := tx.Execute(ctx, `
		CREATE TABLE IF NOT EXISTS `+constants.CacheSchemaName+`.cached_streams (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			from_timestamp INT8,
			last_refreshed INT8,
			cron_schedule TEXT,
			PRIMARY KEY (data_provider, stream_id)
		)`); err != nil {
		return fmt.Errorf("create cached_streams table: %w", err)
	}

	// Create index for efficient querying by cron schedule
	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_streams_cron_schedule 
		ON `+constants.CacheSchemaName+`.cached_streams (cron_schedule)`); err != nil {
		return fmt.Errorf("create cron schedule index: %w", err)
	}

	// Create cached_events table
	if _, err := tx.Execute(ctx, `
		CREATE TABLE IF NOT EXISTS `+constants.CacheSchemaName+`.cached_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			PRIMARY KEY (data_provider, stream_id, event_time)
		)`); err != nil {
		return fmt.Errorf("create cached_events table: %w", err)
	}

	// Create index for efficiently retrieving events by time range
	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_events (data_provider, stream_id, event_time)`); err != nil {
		return fmt.Errorf("create event time range index: %w", err)
	}

	// Create cached_index_events table for storing pre-calculated index values
	if _, err := tx.Execute(ctx, `
		CREATE TABLE IF NOT EXISTS `+constants.CacheSchemaName+`.cached_index_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			PRIMARY KEY (data_provider, stream_id, event_time)
		)`); err != nil {
		return fmt.Errorf("create cached_index_events table: %w", err)
	}

	// Create index for efficiently retrieving index events by time range
	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_index_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_index_events (data_provider, stream_id, event_time)`); err != nil {
		return fmt.Errorf("create index event time range index: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	c.logger.Info("cache schema setup complete")
	return nil
}

// CleanupExtensionSchema removes the cache schema when the extension is disabled
func (c *CacheDB) CleanupExtensionSchema(ctx context.Context) error {
	if c.db == nil {
		c.logger.Warn("cannot cleanup schema: db is nil")
		return nil
	}

	c.logger.Info("cleaning up cache schema")

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Drop schema CASCADE to remove all tables and indexes
	if _, err = tx.Execute(ctx, `DROP SCHEMA IF EXISTS `+constants.CacheSchemaName+` CASCADE`); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	c.logger.Info("cache schema cleaned up successfully")
	return nil
}

// waitForDatabaseReady validates that the database connection is stable before proceeding
func (c *CacheDB) WaitForDatabaseReady(ctx context.Context, maxWait time.Duration) error {
	timeout := time.NewTimer(maxWait)
	defer timeout.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("database not ready after %v", maxWait)
		case <-ticker.C:
			results, err := c.db.Execute(ctx, "SELECT 1")
			if err != nil {
				continue
			}
			// No need to close results - it's not a streaming result
			if len(results.Rows) > 0 {
				return nil // Database is ready
			}
		}
	}
}

// GetLastEventBefore retrieves the most recent event strictly before the specified timestamp.
// Returns (nil, sql.ErrNoRows) if no such event exists.
func (c *CacheDB) GetLastEventBefore(ctx context.Context, dataProvider, streamID string, before int64) (event *CachedEvent, err error) {
	// Add tracing information – we reuse OpDBGetEvents to keep the set of operations small.
	ctx, end := tracing.StreamOperation(ctx, tracing.OpDBGetEvents, dataProvider, streamID,
		attribute.Int64("before", before),
		attribute.String("query", "last_before"))
	defer func() { end(err) }()

	c.logger.Debug("getting last event before timestamp",
		"data_provider", dataProvider,
		"stream_id", streamID,
		"before", before)

	results, err := c.db.Execute(ctx, `
		SELECT data_provider, stream_id, event_time, value
		FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2 AND event_time < $3
		ORDER BY event_time DESC
		LIMIT 1
	`, dataProvider, streamID, before)

	if err != nil {
		return nil, fmt.Errorf("query last event before: %w", err)
	}

	if len(results.Rows) == 0 {
		return nil, sql.ErrNoRows
	}

	if len(results.Rows) != 1 {
		return nil, fmt.Errorf("expected 1 row, got %d", len(results.Rows))
	}

	row := results.Rows[0]
	if len(row) != 4 {
		return nil, fmt.Errorf("expected 4 columns, got %d", len(row))
	}

	dataProviderResult, ok := row[0].(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert data_provider to string")
	}

	streamIDResult, ok := row[1].(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert stream_id to string")
	}

	eventTime, ok := row[2].(int64)
	if !ok {
		return nil, fmt.Errorf("failed to convert event_time to int64")
	}

	value, ok := row[3].(*types.Decimal)
	if !ok {
		return nil, fmt.Errorf("failed to convert value to *types.Decimal")
	}

	e := &CachedEvent{
		DataProvider: dataProviderResult,
		StreamID:     streamIDResult,
		EventTime:    eventTime,
		Value:        value,
	}

	return e, nil
}

// GetFirstEventAfter retrieves the first event at or after the specified timestamp.
// Returns (nil, sql.ErrNoRows) if no such event exists.
func (c *CacheDB) GetFirstEventAfter(ctx context.Context, dataProvider, streamID string, after int64) (event *CachedEvent, err error) {
	ctx, end := tracing.StreamOperation(ctx, tracing.OpDBGetEvents, dataProvider, streamID,
		attribute.Int64("after", after),
		attribute.String("query", "first_after"))
	defer func() { end(err) }()

	c.logger.Debug("getting first event after timestamp",
		"data_provider", dataProvider,
		"stream_id", streamID,
		"after", after)

	results, err := c.db.Execute(ctx, `
		SELECT data_provider, stream_id, event_time, value
		FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		ORDER BY event_time ASC
		LIMIT 1
	`, dataProvider, streamID, after)

	if err != nil {
		return nil, fmt.Errorf("query first event after: %w", err)
	}

	if len(results.Rows) == 0 {
		return nil, sql.ErrNoRows
	}

	if len(results.Rows) != 1 {
		return nil, fmt.Errorf("expected 1 row, got %d", len(results.Rows))
	}

	row := results.Rows[0]
	if len(row) != 4 {
		return nil, fmt.Errorf("expected 4 columns, got %d", len(row))
	}

	dataProviderResult, ok := row[0].(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert data_provider to string")
	}

	streamIDResult, ok := row[1].(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert stream_id to string")
	}

	eventTime, ok := row[2].(int64)
	if !ok {
		return nil, fmt.Errorf("failed to convert event_time to int64")
	}

	value, ok := row[3].(*types.Decimal)
	if !ok {
		return nil, fmt.Errorf("failed to convert value to *types.Decimal")
	}

	e := &CachedEvent{
		DataProvider: dataProviderResult,
		StreamID:     streamIDResult,
		EventTime:    eventTime,
		Value:        value,
	}

	return e, nil
}
