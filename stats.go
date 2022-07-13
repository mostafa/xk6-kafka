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

	WriterWriteTime  *metrics.Metric
	WriterWaitTime   *metrics.Metric
	WriterRetries    *metrics.Metric
	WriterBatchSize  *metrics.Metric
	WriterBatchBytes *metrics.Metric

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
		"kafka.reader.dial.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderFetches, err = registry.NewMetric(
		"kafka.reader.fetches.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMessages, err = registry.NewMetric(
		"kafka.reader.message.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderBytes, err = registry.NewMetric(
		"kafka.reader.message.bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderRebalances, err = registry.NewMetric(
		"kafka.reader.rebalance.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderTimeouts, err = registry.NewMetric(
		"kafka.reader.timeouts.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderErrors, err = registry.NewMetric(
		"kafka.reader.error.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderDialTime, err = registry.NewMetric(
		"kafka.reader.dial.seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderReadTime, err = registry.NewMetric(
		"kafka.reader.read.seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderWaitTime, err = registry.NewMetric(
		"kafka.reader.wait.seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderFetchSize, err = registry.NewMetric(
		"kafka.reader.fetch.size", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderFetchBytes, err = registry.NewMetric(
		"kafka.reader.fetch.bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderOffset, err = registry.NewMetric(
		"kafka.reader.offset", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderLag, err = registry.NewMetric(
		"kafka.reader.lag", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMinBytes, err = registry.NewMetric(
		"kafka.reader.fetch_bytes.min", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMaxBytes, err = registry.NewMetric(
		"kafka.reader.fetch_bytes.max", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderMaxWait, err = registry.NewMetric(
		"kafka.reader.fetch_wait.max", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderQueueLength, err = registry.NewMetric(
		"kafka.reader.queue.length", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.ReaderQueueCapacity, err = registry.NewMetric(
		"kafka.reader.queue.capacity", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWrites, err = registry.NewMetric(
		"kafka.writer.write.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterMessages, err = registry.NewMetric(
		"kafka.writer.message.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBytes, err = registry.NewMetric(
		"kafka.writer.message.bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterErrors, err = registry.NewMetric(
		"kafka.writer.error.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWriteTime, err = registry.NewMetric(
		"kafka.writer.write.seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWaitTime, err = registry.NewMetric(
		"kafka.writer.wait.seconds", metrics.Trend, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterRetries, err = registry.NewMetric(
		"kafka.writer.retries.count", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchSize, err = registry.NewMetric(
		"kafka.writer.batch.size", metrics.Counter); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchBytes, err = registry.NewMetric(
		"kafka.writer.batch.bytes", metrics.Counter, metrics.Data); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterMaxAttempts, err = registry.NewMetric(
		"kafka.writer.attempts.max", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterMaxBatchSize, err = registry.NewMetric(
		"kafka.writer.batch.max", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterBatchTimeout, err = registry.NewMetric(
		"kafka.writer.batch.timeout", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterReadTimeout, err = registry.NewMetric(
		"kafka.writer.read.timeout", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterWriteTimeout, err = registry.NewMetric(
		"kafka.writer.write.timeout", metrics.Gauge, metrics.Time); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterRequiredAcks, err = registry.NewMetric(
		"kafka.writer.acks.required", metrics.Gauge); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	if kafkaMetrics.WriterAsync, err = registry.NewMetric(
		"kafka.writer.async", metrics.Rate); err != nil {
		return kafkaMetrics, errors.Unwrap(err)
	}

	return kafkaMetrics, nil
}
