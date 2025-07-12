package metrics

import (
	"context"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OTELMetrics implements MetricsRecorder using OpenTelemetry
type OTELMetrics struct {
	// Cache effectiveness metrics
	cacheHits       metric.Int64Counter
	cacheMisses     metric.Int64Counter
	cacheDataServed metric.Int64Histogram
	cacheDataAge    metric.Float64Histogram

	// Refresh operation metrics
	refreshDuration metric.Float64Histogram
	refreshEvents   metric.Int64Histogram
	refreshErrors   metric.Int64Counter
	lastRefresh     metric.Float64Gauge

	// Resource metrics
	streamsConfigured metric.Int64Gauge
	streamsActive     metric.Int64Gauge
	cacheEventCount   metric.Int64Gauge

	// Resolution metrics
	resolutionDuration         metric.Float64Histogram
	resolutionErrors           metric.Int64Counter
	resolutionStreamsDiscovered metric.Int64Counter
	resolutionStreamsRemoved   metric.Int64Counter

	// Refresh skip metrics
	refreshSkipped metric.Int64Counter

	logger log.Logger
}

// NewOTELMetrics creates a new OpenTelemetry metrics recorder
func NewOTELMetrics(meter metric.Meter, logger log.Logger) (*OTELMetrics, error) {
	m := &OTELMetrics{logger: logger}

	var err error

	// Cache effectiveness metrics
	m.cacheHits, err = meter.Int64Counter("tn_cache.hits",
		metric.WithDescription("Number of cache hits"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.cacheMisses, err = meter.Int64Counter("tn_cache.misses",
		metric.WithDescription("Number of cache misses"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.cacheDataServed, err = meter.Int64Histogram("tn_cache.data_served",
		metric.WithDescription("Number of rows served from cache"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.cacheDataAge, err = meter.Float64Histogram("tn_cache.data_age",
		metric.WithDescription("Age of cached data when served"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	// Refresh operation metrics
	m.refreshDuration, err = meter.Float64Histogram("tn_cache.refresh.duration",
		metric.WithDescription("Time taken to refresh cache"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	m.refreshEvents, err = meter.Int64Histogram("tn_cache.refresh.events",
		metric.WithDescription("Number of events cached per refresh"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.refreshErrors, err = meter.Int64Counter("tn_cache.refresh.errors",
		metric.WithDescription("Number of refresh errors"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.lastRefresh, err = meter.Float64Gauge("tn_cache.refresh.last_success",
		metric.WithDescription("Timestamp of last successful refresh"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	// Resource metrics
	m.streamsConfigured, err = meter.Int64Gauge("tn_cache.streams.configured",
		metric.WithDescription("Number of streams configured for caching"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.streamsActive, err = meter.Int64Gauge("tn_cache.streams.active",
		metric.WithDescription("Number of streams successfully cached"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.cacheEventCount, err = meter.Int64Gauge("tn_cache.events.total",
		metric.WithDescription("Total number of cached events per stream"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	// Resolution metrics
	m.resolutionDuration, err = meter.Float64Histogram("tn_cache.resolution.duration",
		metric.WithDescription("Time taken to resolve wildcards"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	m.resolutionErrors, err = meter.Int64Counter("tn_cache.resolution.errors",
		metric.WithDescription("Number of resolution errors"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.resolutionStreamsDiscovered, err = meter.Int64Counter("tn_cache.resolution.streams_discovered",
		metric.WithDescription("Number of streams discovered during resolution"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.resolutionStreamsRemoved, err = meter.Int64Counter("tn_cache.resolution.streams_removed",
		metric.WithDescription("Number of streams removed during resolution"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m.refreshSkipped, err = meter.Int64Counter("tn_cache.refresh.skipped",
		metric.WithDescription("Number of refresh operations skipped"),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Implementation of MetricsRecorder interface

func (m *OTELMetrics) RecordCacheHit(ctx context.Context, dataProvider, streamID string) {
	m.cacheHits.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordCacheMiss(ctx context.Context, dataProvider, streamID string) {
	m.cacheMisses.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordCacheDataServed(ctx context.Context, dataProvider, streamID string, rowCount int) {
	m.cacheDataServed.Record(ctx, int64(rowCount),
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordCacheDataAge(ctx context.Context, dataProvider, streamID string, ageSeconds float64) {
	m.cacheDataAge.Record(ctx, ageSeconds,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordRefreshStart(ctx context.Context, dataProvider, streamID string) {
	// Could add a gauge for "refreshes in progress" if needed
}

func (m *OTELMetrics) RecordRefreshComplete(ctx context.Context, dataProvider, streamID string, duration time.Duration, eventCount int) {
	m.refreshDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))

	m.refreshEvents.Record(ctx, int64(eventCount),
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))

	m.lastRefresh.Record(ctx, float64(time.Now().Unix()),
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordRefreshError(ctx context.Context, dataProvider, streamID string, errType string) {
	m.refreshErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
			attribute.String("error_type", errType),
		))
}

func (m *OTELMetrics) RecordStreamConfigured(ctx context.Context, count int) {
	m.streamsConfigured.Record(ctx, int64(count))
}

func (m *OTELMetrics) RecordStreamActive(ctx context.Context, count int) {
	m.streamsActive.Record(ctx, int64(count))
}

func (m *OTELMetrics) RecordCacheSize(ctx context.Context, dataProvider, streamID string, eventCount int64) {
	m.cacheEventCount.Record(ctx, eventCount,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordResolutionDuration(ctx context.Context, duration time.Duration, streamCount int) {
	m.resolutionDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.Int("stream_count", streamCount),
		))
}

func (m *OTELMetrics) RecordResolutionError(ctx context.Context, errType string) {
	m.resolutionErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error_type", errType),
		))
}

func (m *OTELMetrics) RecordResolutionStreamDiscovered(ctx context.Context, dataProvider, streamID string) {
	m.resolutionStreamsDiscovered.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordResolutionStreamRemoved(ctx context.Context, dataProvider, streamID string) {
	m.resolutionStreamsRemoved.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
		))
}

func (m *OTELMetrics) RecordRefreshSkipped(ctx context.Context, dataProvider, streamID, reason string) {
	m.refreshSkipped.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("data_provider", dataProvider),
			attribute.String("stream_id", streamID),
			attribute.String("reason", reason),
		))
}