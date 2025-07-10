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

	"database/sql"

	"github.com/jackc/pgx/v5"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/parsing"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
)

// DBPool interface wraps the database pool methods we use
type DBPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CacheDB handles database operations for the TRUF.NETWORK cache
type CacheDB struct {
	logger log.Logger
	pool   DBPool // Independent connection pool
}

// GetPool returns the underlying database pool
func (c *CacheDB) GetPool() DBPool {
	return c.pool
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

// CachedIndexEvent represents a cached index event from a stream
type CachedIndexEvent struct {
	DataProvider string
	StreamID     string
	EventTime    int64
	Value        *types.Decimal // The pre-calculated index value
}

// NewCacheDB creates a new CacheDB instance with DBPool
func NewCacheDB(pool DBPool, logger log.Logger) *CacheDB {
	return &CacheDB{
		logger: logger.New("tn_cache_db"),
		pool:   pool,
	}
}

// AddStreamConfig adds or updates a stream's cache configuration
func (c *CacheDB) AddStreamConfig(ctx context.Context, config StreamCacheConfig) error {
	c.logger.Debug("adding stream cache config",
		"data_provider", config.DataProvider,
		"stream_id", config.StreamID)

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert or update the stream config using UPSERT
	_, err = tx.Exec(ctx, `
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

	tx, err := c.pool.Begin(ctx)
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
	_, err = tx.Exec(ctx, `
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

	rows, err := c.pool.Query(ctx, `
		SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule
		FROM `+constants.CacheSchemaName+`.cached_streams
		ORDER BY data_provider, stream_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query stream configs: %w", err)
	}
	defer rows.Close()

	var configs []StreamCacheConfig
	for rows.Next() {
		var config StreamCacheConfig
		err := rows.Scan(
			&config.DataProvider,
			&config.StreamID,
			&config.FromTimestamp,
			&config.LastRefreshed,
			&config.CronSchedule,
		)
		if err != nil {
			return nil, fmt.Errorf("scan stream config: %w", err)
		}
		configs = append(configs, config)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stream configs: %w", err)
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
	tx, err := c.pool.Begin(ctx)
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
		_, err = tx.Exec(ctx, `
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
		_, err = tx.Exec(ctx, `
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
func (c *CacheDB) CacheEventsWithIndex(ctx context.Context, events []CachedEvent, indexEvents []CachedIndexEvent) (err error) {
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
	tx, err := c.pool.Begin(ctx)
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
			_, err = tx.Exec(ctx, `
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
			_, err = tx.Exec(ctx, `
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
		_, err = tx.Exec(ctx, `
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

// GetEvents retrieves events from the cache for a specific time range
func (c *CacheDB) GetEvents(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) (events []CachedEvent, err error) {
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

	var rows pgx.Rows
	if toTime > 0 {
		rows, err = c.pool.Query(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3 AND event_time <= $4
			ORDER BY event_time
		`, dataProvider, streamID, fromTime, toTime)
	} else {
		rows, err = c.pool.Query(ctx, `
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
	defer rows.Close()

	events = make([]CachedEvent, 0)
	for rows.Next() {
		var event CachedEvent
		var valueRaw interface{}

		err := rows.Scan(&event.DataProvider, &event.StreamID, &event.EventTime, &valueRaw)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}

		// Parse the value using the shared parsing logic
		value, err := parsing.ParseEventValue(valueRaw)
		if err != nil {
			return nil, fmt.Errorf("parse event value: %w", err)
		}
		event.Value = value

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}

	return events, nil
}

// GetIndexEvents retrieves index events from the cache for a specific time range
func (c *CacheDB) GetIndexEvents(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) (events []CachedIndexEvent, err error) {
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

	var rows pgx.Rows
	if toTime > 0 {
		rows, err = c.pool.Query(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_index_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3 AND event_time <= $4
			ORDER BY event_time
		`, dataProvider, streamID, fromTime, toTime)
	} else {
		rows, err = c.pool.Query(ctx, `
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
	defer rows.Close()

	events = make([]CachedIndexEvent, 0)
	for rows.Next() {
		var event CachedIndexEvent
		var valueRaw interface{}

		err := rows.Scan(&event.DataProvider, &event.StreamID, &event.EventTime, &valueRaw)
		if err != nil {
			return nil, fmt.Errorf("scan index event: %w", err)
		}

		// Parse the index value using the shared parsing logic
		value, err := parsing.ParseEventValue(valueRaw)
		if err != nil {
			return nil, fmt.Errorf("parse index value: %w", err)
		}
		event.Value = value

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate index events: %w", err)
	}

	return events, nil
}

// DeleteStreamData deletes all cached events for a stream
func (c *CacheDB) DeleteStreamData(ctx context.Context, dataProvider, streamID string) error {
	c.logger.Debug("deleting stream data",
		"data_provider", dataProvider,
		"stream_id", streamID)

	tx, err := c.pool.Begin(ctx)
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
	_, err = tx.Exec(ctx, `
		DELETE FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return fmt.Errorf("delete stream events: %w", err)
	}

	// Delete index events for the stream
	_, err = tx.Exec(ctx, `
		DELETE FROM `+constants.CacheSchemaName+`.cached_index_events
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return fmt.Errorf("delete stream index events: %w", err)
	}

	// Delete stream configuration
	_, err = tx.Exec(ctx, `
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

	tx, err := c.pool.Begin(ctx)
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
	_, err = tx.Exec(ctx, `DELETE FROM `+constants.CacheSchemaName+`.cached_events`)
	if err != nil {
		return fmt.Errorf("delete all events: %w", err)
	}

	// Delete all index events
	_, err = tx.Exec(ctx, `DELETE FROM `+constants.CacheSchemaName+`.cached_index_events`)
	if err != nil {
		return fmt.Errorf("delete all index events: %w", err)
	}

	// Delete all stream configurations
	_, err = tx.Exec(ctx, `DELETE FROM `+constants.CacheSchemaName+`.cached_streams`)
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

	tx, err := c.pool.Begin(ctx)
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
	var streamExists bool
	err = tx.QueryRow(ctx, `
		SELECT COUNT(*) > 0
		FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
			AND (from_timestamp IS NULL OR from_timestamp <= $3)
	`, dataProvider, streamID, fromTime).Scan(&streamExists)

	if err != nil {
		return false, fmt.Errorf("check stream config: %w", err)
	}

	if !streamExists {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit transaction: %w", err)
		}
		return false, nil
	}

	// Then, check if there are events in the cache
	var eventsExist bool
	if toTime > 0 {
		err = tx.QueryRow(ctx, `
			SELECT COUNT(*) > 0
			FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3 AND event_time <= $4
		`, dataProvider, streamID, fromTime, toTime).Scan(&eventsExist)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT COUNT(*) > 0
			FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3
		`, dataProvider, streamID, fromTime).Scan(&eventsExist)
	}

	if err != nil {
		return false, fmt.Errorf("check events: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit transaction: %w", err)
	}

	return eventsExist, nil
}

// UpdateStreamConfigsAtomic atomically updates the cached_streams table
// It adds/updates configs in newConfigs and deletes configs in toDelete
func (c *CacheDB) UpdateStreamConfigsAtomic(ctx context.Context, newConfigs []StreamCacheConfig, toDelete []StreamCacheConfig) error {
	c.logger.Debug("atomic stream config update",
		"new_count", len(newConfigs),
		"delete_count", len(toDelete))

	tx, err := c.pool.Begin(ctx)
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
		_, err = tx.Exec(ctx, `
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
		_, err = tx.Exec(ctx, `
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
	row := c.pool.QueryRow(ctx, `
		SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule
		FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	var config StreamCacheConfig
	err := row.Scan(
		&config.DataProvider,
		&config.StreamID,
		&config.FromTimestamp,
		&config.LastRefreshed,
		&config.CronSchedule,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("query stream config: %w", err)
	}

	return &config, nil
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

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query cached streams with counts: %w", err)
	}
	defer rows.Close()

	var results []StreamCountInfo
	for rows.Next() {
		var info StreamCountInfo
		err := rows.Scan(&info.DataProvider, &info.StreamID, &info.EventCount)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		results = append(results, info)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	return results, nil
}
