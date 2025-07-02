package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
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
	Value        float64
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

	// Insert or update the stream config
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

	// Insert events in batches to avoid parameter limits
	batchSize := 100
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]
		// Build the query for the batch
		query := `
			INSERT INTO ext_tn_cache.cached_events 
				(data_provider, stream_id, event_time, value)
			VALUES 
		`
		args := make([]interface{}, 0, len(batch)*4)
		for j, event := range batch {
			if j > 0 {
				query += ", "
			}
			paramBase := j * 4
			query += fmt.Sprintf("($%d, $%d, $%d, $%d)",
				paramBase+1, paramBase+2, paramBase+3, paramBase+4)
			args = append(args, event.DataProvider, event.StreamID, event.EventTime, event.Value)
		}
		query += `
			ON CONFLICT (data_provider, stream_id, event_time) 
			DO UPDATE SET
				value = EXCLUDED.value
		`

		_, err = tx.Execute(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("insert events batch: %w", err)
		}
	}

	// Update the last refreshed timestamp for the stream
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
		event := CachedEvent{
			DataProvider: row[0].(string),
			StreamID:     row[1].(string),
			EventTime:    row[2].(int64),
			Value:        row[3].(float64),
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

	// First check if the stream is configured for caching
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

	// Then check if there are events in the cache
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
