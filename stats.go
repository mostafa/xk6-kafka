package kafka

import (
	"errors"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

type kafkaMetrics struct {
	ReaderDials      *metrics.Metric
	ReaderFetches    *metrics.Metric
	ReaderMessages   *metrics.Metric
	ReaderBytes      *metrics.Metric
	ReaderRebalances *metrics.Metric
	ReaderTimeouts   *metrics.Metric
	ReaderErrors     *metrics.Metric

	ReaderDialTime   *metrics.Metric
	ReaderReadTime   *metrics.Metric
	ReaderWaitTime   *metrics.Metric
	ReaderFetchSize  *metrics.Metric
	ReaderFetchBytes *metrics.Metric

	ReaderOffset        *metrics.Metric
	ReaderLag           *metrics.Metric
	ReaderMinBytes      *metrics.Metric
	ReaderMaxBytes      *metrics.Metric
	ReaderMaxWait       *metrics.Metric
	ReaderQueueLength   *metrics.Metric
	ReaderQueueCapacity *metrics.Metric

	WriterWrites   *metrics.Metric
	WriterMessages *metrics.Metric
	WriterBytes    *metrics.Metric
	WriterErrors   *metrics.Metric

	WriterBatchTime      *metrics.Metric
	WriterBatchQueueTime *metrics.Metric
	WriterWriteTime      *metrics.Metric
	WriterWaitTime       *metrics.Metric
	WriterRetries        *metrics.Metric
	WriterBatchSize      *metrics.Metric
	WriterBatchBytes     *metrics.Metric

	WriterMaxAttempts  *metrics.Metric
	WriterMaxBatchSize *metrics.Metric
	WriterBatchTimeout *metrics.Metric
	WriterReadTimeout  *metrics.Metric
	WriterWriteTimeout *metrics.Metric
	WriterRequiredAcks *metrics.Metric
	WriterAsync        *metrics.Metric
}

// registerMetrics registers the metrics for the kafka module in the metrics registry
// nolint: funlen,maintidx
func registerMetrics(vu modules.VU) (kafkaMetrics, error) {
	var err error
	registry := vu.InitEnv().Registry
	kafkaMetrics := kafkaMetrics{}

	if kafkaMetrics.ReaderDials, err = registry.NewMetric(
		"kafka_reader_dial_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderFetches, err = registry.NewMetric(
		"kafka_reader_fetches_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMessages, err = registry.NewMetric(
		"kafka_reader_message_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderBytes, err = registry.NewMetric(
		"kafka_reader_message_bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderRebalances, err = registry.NewMetric(
		"kafka_reader_rebalance_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderTimeouts, err = registry.NewMetric(
		"kafka_reader_timeouts_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderErrors, err = registry.NewMetric(
		"kafka_reader_error_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderDialTime, err = registry.NewMetric(
		"kafka_reader_dial_seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderReadTime, err = registry.NewMetric(
		"kafka_reader_read_seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderWaitTime, err = registry.NewMetric(
		"kafka_reader_wait_seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderFetchSize, err = registry.NewMetric(
		"kafka_reader_fetch_size", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderFetchBytes, err = registry.NewMetric(
		"kafka_reader_fetch_bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderOffset, err = registry.NewMetric(
		"kafka_reader_offset", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderLag, err = registry.NewMetric(
		"kafka_reader_lag", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMinBytes, err = registry.NewMetric(
		"kafka_reader_fetch_bytes_min", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMaxBytes, err = registry.NewMetric(
		"kafka_reader_fetch_bytes_max", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMaxWait, err = registry.NewMetric(
		"kafka_reader_fetch_wait_max", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderQueueLength, err = registry.NewMetric(
		"kafka_reader_queue_length", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderQueueCapacity, err = registry.NewMetric(
		"kafka_reader_queue_capacity", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWrites, err = registry.NewMetric(
		"kafka_writer_write_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterMessages, err = registry.NewMetric(
		"kafka_writer_message_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBytes, err = registry.NewMetric(
		"kafka_writer_message_bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterErrors, err = registry.NewMetric(
		"kafka_writer_error_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchTime, err = registry.NewMetric(
		"kafka_writer_batch_seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchQueueTime, err = registry.NewMetric(
		"kafka_writer_batch_queue_seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWriteTime, err = registry.NewMetric(
		"kafka_writer_write_seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWaitTime, err = registry.NewMetric(
		"kafka_writer_wait_seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterRetries, err = registry.NewMetric(
		"kafka_writer_retries_count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchSize, err = registry.NewMetric(
		"kafka_writer_batch_size", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchBytes, err = registry.NewMetric(
		"kafka_writer_batch_bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterMaxAttempts, err = registry.NewMetric(
		"kafka_writer_attempts_max", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterMaxBatchSize, err = registry.NewMetric(
		"kafka_writer_batch_max", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchTimeout, err = registry.NewMetric(
		"kafka_writer_batch_timeout", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterReadTimeout, err = registry.NewMetric(
		"kafka_writer_read_timeout", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWriteTimeout, err = registry.NewMetric(
		"kafka_writer_write_timeout", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterRequiredAcks, err = registry.NewMetric(
		"kafka_writer_acks_required", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterAsync, err = registry.NewMetric(
		"kafka_writer_async", metrics.Rate); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	return kafkaMetrics, nil
}
