// Package internal provides database operations for the TN Cache extension.
//
// IMPORTANT: Table Ownership Design
//
// The TN Cache extension is designed to be the EXCLUSIVE manager of the tables it creates:
// - ext_tn_cache.cached_events
// - ext_tn_cache.cached_streams
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
	"math/big"
	"strconv"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// CacheDB handles database operations for the TRUF.NETWORK cache
type CacheDB struct {
	logger log.Logger
	db     sql.DB
}

// StreamCacheConfig represents a stream's caching configuration
type StreamCacheConfig struct {
	DataProvider  string
	StreamID      string
	FromTimestamp int64
	LastRefreshed string
	CronSchedule  string
}

// CachedEvent represents a cached event from a stream
type CachedEvent struct {
	DataProvider string
	StreamID     string
	EventTime    int64
	Value        *types.Decimal // Use high-precision decimal for decimal(36,18) values
}

// NewCacheDB creates a new CacheDB instance
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
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Insert or update the stream config using UPSERT
	_, err = tx.Execute(ctx, `
		INSERT INTO ext_tn_cache.cached_streams 
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
	var fromTimestamps []int64
	var lastRefresheds []string

	for _, config := range configs {
		dataProviders = append(dataProviders, config.DataProvider)
		streamIDs = append(streamIDs, config.StreamID)
		fromTimestamps = append(fromTimestamps, config.FromTimestamp)
		lastRefresheds = append(lastRefresheds, config.LastRefreshed)
		cronSchedules = append(cronSchedules, config.CronSchedule)
	}

	// Execute the batch insert with UPSERT logic
	_, err = tx.Execute(ctx, `
		INSERT INTO ext_tn_cache.cached_streams 
			(data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
		SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::INT8[], $4::TEXT[], $5::TEXT[])
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

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	result, err := tx.Execute(ctx, `
		SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule
		FROM ext_tn_cache.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return nil, fmt.Errorf("query stream config: %w", err)
	}

	if len(result.Rows) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit transaction: %w", err)
		}
		return nil, sql.ErrNoRows
	}

	row := result.Rows[0]
	config := &StreamCacheConfig{
		DataProvider:  row[0].(string),
		StreamID:      row[1].(string),
		FromTimestamp: row[2].(int64),
		LastRefreshed: row[3].(string),
		CronSchedule:  row[4].(string),
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return config, nil
}

// ListStreamConfigs retrieves all stream cache configurations
func (c *CacheDB) ListStreamConfigs(ctx context.Context) ([]StreamCacheConfig, error) {
	c.logger.Debug("listing all stream cache configs")

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	result, err := tx.Execute(ctx, `
		SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule
		FROM ext_tn_cache.cached_streams
		ORDER BY data_provider, stream_id
	`)

	if err != nil {
		return nil, fmt.Errorf("query stream configs: %w", err)
	}

	configs := make([]StreamCacheConfig, 0, len(result.Rows))
	for _, row := range result.Rows {
		config := StreamCacheConfig{
			DataProvider:  row[0].(string),
			StreamID:      row[1].(string),
			FromTimestamp: row[2].(int64),
			LastRefreshed: row[3].(string),
			CronSchedule:  row[4].(string),
		}
		configs = append(configs, config)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return configs, nil
}

// CacheEvents stores events in the cache
func (c *CacheDB) CacheEvents(ctx context.Context, events []CachedEvent) error {
	if len(events) == 0 {
		return nil
	}

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
			INSERT INTO ext_tn_cache.cached_events 
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
		now := time.Now().UTC().Format(time.RFC3339)
		_, err = tx.Execute(ctx, `
			UPDATE ext_tn_cache.cached_streams
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

// GetEvents retrieves events from the cache for a specific time range
func (c *CacheDB) GetEvents(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) ([]CachedEvent, error) {
	c.logger.Debug("getting cached events",
		"data_provider", dataProvider,
		"stream_id", streamID,
		"from_time", fromTime,
		"to_time", toTime)

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	var result *sql.ResultSet
	if toTime > 0 {
		result, err = tx.Execute(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM ext_tn_cache.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3 AND event_time <= $4
			ORDER BY event_time
		`, dataProvider, streamID, fromTime, toTime)
	} else {
		result, err = tx.Execute(ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM ext_tn_cache.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3
			ORDER BY event_time
		`, dataProvider, streamID, fromTime)
	}

	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}

	events := make([]CachedEvent, 0, len(result.Rows))
	for _, row := range result.Rows {
		// Parse the value using the same logic as refresh.go
		value, err := parseEventValueFromDB(row[3])
		if err != nil {
			return nil, fmt.Errorf("parse event value: %w", err)
		}

		event := CachedEvent{
			DataProvider: row[0].(string),
			StreamID:     row[1].(string),
			EventTime:    row[2].(int64),
			Value:        value,
		}
		events = append(events, event)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
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
		DELETE FROM ext_tn_cache.cached_events
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)

	if err != nil {
		return fmt.Errorf("delete stream events: %w", err)
	}

	// Delete stream configuration
	_, err = tx.Execute(ctx, `
		DELETE FROM ext_tn_cache.cached_streams
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
	_, err = tx.Execute(ctx, `DELETE FROM ext_tn_cache.cached_events`)
	if err != nil {
		return fmt.Errorf("delete all events: %w", err)
	}

	// Delete all stream configurations
	_, err = tx.Execute(ctx, `DELETE FROM ext_tn_cache.cached_streams`)
	if err != nil {
		return fmt.Errorf("delete all stream configs: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// HasCachedData checks if there is cached data for a stream in a time range
func (c *CacheDB) HasCachedData(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) (bool, error) {
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
	streamResult, err := tx.Execute(ctx, `
		SELECT COUNT(*) > 0
		FROM ext_tn_cache.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
			AND (from_timestamp IS NULL OR from_timestamp <= $3)
	`, dataProvider, streamID, fromTime)

	if err != nil {
		return false, fmt.Errorf("check stream config: %w", err)
	}

	if len(streamResult.Rows) == 0 || !streamResult.Rows[0][0].(bool) {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit transaction: %w", err)
		}
		return false, nil
	}

	// Then, check if there are events in the cache
	var eventsResult *sql.ResultSet
	if toTime > 0 {
		eventsResult, err = tx.Execute(ctx, `
			SELECT COUNT(*) > 0
			FROM ext_tn_cache.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3 AND event_time <= $4
		`, dataProvider, streamID, fromTime, toTime)
	} else {
		eventsResult, err = tx.Execute(ctx, `
			SELECT COUNT(*) > 0
			FROM ext_tn_cache.cached_events
			WHERE data_provider = $1 AND stream_id = $2
				AND event_time >= $3
		`, dataProvider, streamID, fromTime)
	}

	if err != nil {
		return false, fmt.Errorf("check events: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit transaction: %w", err)
	}

	return len(eventsResult.Rows) > 0 && eventsResult.Rows[0][0].(bool), nil
}

// parseEventValueFromDB converts database values to *types.Decimal with decimal(36,18) precision
// This handles values that come back from the database which may be in different formats
func parseEventValueFromDB(v interface{}) (*types.Decimal, error) {
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
	case float64:
		// Convert float64 to decimal(36,18) - note: may lose precision if > 15 digits
		return types.ParseDecimalExplicit(strconv.FormatFloat(val, 'f', -1, 64), 36, 18)
	case float32:
		return types.ParseDecimalExplicit(strconv.FormatFloat(float64(val), 'f', -1, 32), 36, 18)
	case int64:
		return types.ParseDecimalExplicit(strconv.FormatInt(val, 10), 36, 18)
	case int:
		return types.ParseDecimalExplicit(strconv.Itoa(val), 36, 18)
	case int32:
		return types.ParseDecimalExplicit(strconv.FormatInt(int64(val), 10), 36, 18)
	case uint64:
		return types.ParseDecimalExplicit(strconv.FormatUint(val, 10), 36, 18)
	case uint32:
		return types.ParseDecimalExplicit(strconv.FormatUint(uint64(val), 10), 36, 18)
	case *big.Int:
		if val == nil {
			return nil, fmt.Errorf("nil big.Int value")
		}
		return types.ParseDecimalExplicit(val.String(), 36, 18)
	case string:
		// Parse string directly as decimal(36,18)
		return types.ParseDecimalExplicit(val, 36, 18)
	case []byte:
		// Handle potential byte array from database
		return types.ParseDecimalExplicit(string(val), 36, 18)
	case nil:
		return nil, fmt.Errorf("nil value")
	default:
		return nil, fmt.Errorf("unsupported database value type: %T", v)
	}
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
			DELETE FROM ext_tn_cache.cached_streams
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
		var fromTimestamps []int64
		var lastRefresheds []string

		for _, config := range newConfigs {
			dataProviders = append(dataProviders, config.DataProvider)
			streamIDs = append(streamIDs, config.StreamID)
			fromTimestamps = append(fromTimestamps, config.FromTimestamp)
			lastRefresheds = append(lastRefresheds, config.LastRefreshed)
			cronSchedules = append(cronSchedules, config.CronSchedule)
		}

		// Execute the batch upsert, preserving last_refreshed if it exists
		_, err = tx.Execute(ctx, `
			INSERT INTO ext_tn_cache.cached_streams 
				(data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
			SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::INT8[], $4::TEXT[], $5::TEXT[])
			ON CONFLICT (data_provider, stream_id) 
			DO UPDATE SET
				from_timestamp = EXCLUDED.from_timestamp,
				last_refreshed = COALESCE(NULLIF(EXCLUDED.last_refreshed, ''), cached_streams.last_refreshed),
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
