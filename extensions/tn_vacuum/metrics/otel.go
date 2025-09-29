package metrics

import (
	"context"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OTELMetrics implements MetricsRecorder using OpenTelemetry.
type OTELMetrics struct {
	logger log.Logger

	// Counters
	vacuumStartCounter    metric.Int64Counter
	vacuumCompleteCounter metric.Int64Counter
	vacuumErrorCounter    metric.Int64Counter
	vacuumSkippedCounter  metric.Int64Counter

	// Histograms
	vacuumDuration  metric.Float64Histogram
	tablesProcessed metric.Int64Histogram

	// Gauges
	lastRunHeight metric.Int64Gauge
}

// NewOTELMetrics creates a new OTEL metrics recorder.
func NewOTELMetrics(meter metric.Meter, logger log.Logger) (*OTELMetrics, error) {
	m := &OTELMetrics{logger: logger}

	var err error

	// Create counters
	m.vacuumStartCounter, err = meter.Int64Counter(
		"tn_vacuum.vacuum_start_total",
		metric.WithDescription("Total number of vacuum operations started"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}

	m.vacuumCompleteCounter, err = meter.Int64Counter(
		"tn_vacuum.vacuum_complete_total",
		metric.WithDescription("Total number of vacuum operations completed successfully"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}

	m.vacuumErrorCounter, err = meter.Int64Counter(
		"tn_vacuum.vacuum_error_total",
		metric.WithDescription("Total number of vacuum errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	m.vacuumSkippedCounter, err = meter.Int64Counter(
		"tn_vacuum.vacuum_skipped_total",
		metric.WithDescription("Total number of vacuum operations skipped"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}

	// Create histograms
	m.vacuumDuration, err = meter.Float64Histogram(
		"tn_vacuum.vacuum_duration_seconds",
		metric.WithDescription("Duration of vacuum operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.tablesProcessed, err = meter.Int64Histogram(
		"tn_vacuum.tables_processed",
		metric.WithDescription("Number of tables processed during vacuum"),
		metric.WithUnit("{table}"),
	)
	if err != nil {
		return nil, err
	}

	// Create gauges
	m.lastRunHeight, err = meter.Int64Gauge(
		"tn_vacuum.last_run_height",
		metric.WithDescription("Block height of the last vacuum run"),
		metric.WithUnit("{block}"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *OTELMetrics) RecordVacuumStart(ctx context.Context, mechanism string) {
	m.vacuumStartCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("mechanism", mechanism),
		),
	)
}

func (m *OTELMetrics) RecordVacuumComplete(ctx context.Context, mechanism string, duration time.Duration, tablesProcessed int) {
	attrs := metric.WithAttributes(
		attribute.String("mechanism", mechanism),
	)

	m.vacuumCompleteCounter.Add(ctx, 1, attrs)
	m.vacuumDuration.Record(ctx, duration.Seconds(), attrs)
	m.tablesProcessed.Record(ctx, int64(tablesProcessed), attrs)
}

func (m *OTELMetrics) RecordVacuumError(ctx context.Context, mechanism string, errType string) {
	m.vacuumErrorCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("mechanism", mechanism),
			attribute.String("error_type", errType),
		),
	)
}

func (m *OTELMetrics) RecordVacuumSkipped(ctx context.Context, reason string) {
	m.vacuumSkippedCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("reason", reason),
		),
	)
}

func (m *OTELMetrics) RecordLastRunHeight(ctx context.Context, height int64) {
	m.lastRunHeight.Record(ctx, height)
}

