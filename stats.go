package kafka

import (
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
// nolint: funlen
func registerMetrics(vu modules.VU) (kafkaMetrics, error) {
	var err error
	registry := vu.InitEnv().Registry
	m := kafkaMetrics{}

	if m.ReaderDials, err = registry.NewMetric("kafka.reader.dial.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.ReaderFetches, err = registry.NewMetric("kafka.reader.fetches.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.ReaderMessages, err = registry.NewMetric("kafka.reader.message.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.ReaderBytes, err = registry.NewMetric("kafka.reader.message.bytes", metrics.Counter, metrics.Data); err != nil {
		return m, err
	}

	if m.ReaderRebalances, err = registry.NewMetric("kafka.reader.rebalance.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.ReaderTimeouts, err = registry.NewMetric("kafka.reader.timeouts.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.ReaderErrors, err = registry.NewMetric("kafka.reader.error.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.ReaderDialTime, err = registry.NewMetric("kafka.reader.dial.seconds", metrics.Trend, metrics.Time); err != nil {
		return m, err
	}

	if m.ReaderReadTime, err = registry.NewMetric("kafka.reader.read.seconds", metrics.Trend, metrics.Time); err != nil {
		return m, err
	}

	if m.ReaderWaitTime, err = registry.NewMetric("kafka.reader.wait.seconds", metrics.Trend, metrics.Time); err != nil {
		return m, err
	}

	if m.ReaderFetchSize, err = registry.NewMetric("kafka.reader.fetch.size", metrics.Counter); err != nil {
		return m, err
	}

	if m.ReaderFetchBytes, err = registry.NewMetric("kafka.reader.fetch.bytes", metrics.Counter, metrics.Data); err != nil {
		return m, err
	}

	if m.ReaderOffset, err = registry.NewMetric("kafka.reader.offset", metrics.Gauge); err != nil {
		return m, err
	}

	if m.ReaderLag, err = registry.NewMetric("kafka.reader.lag", metrics.Gauge); err != nil {
		return m, err
	}

	if m.ReaderMinBytes, err = registry.NewMetric("kafka.reader.fetch_bytes.min", metrics.Gauge); err != nil {
		return m, err
	}

	if m.ReaderMaxBytes, err = registry.NewMetric("kafka.reader.fetch_bytes.max", metrics.Gauge); err != nil {
		return m, err
	}

	if m.ReaderMaxWait, err = registry.NewMetric("kafka.reader.fetch_wait.max", metrics.Gauge, metrics.Time); err != nil {
		return m, err
	}

	if m.ReaderQueueLength, err = registry.NewMetric("kafka.reader.queue.length", metrics.Gauge); err != nil {
		return m, err
	}

	if m.ReaderQueueCapacity, err = registry.NewMetric("kafka.reader.queue.capacity", metrics.Gauge); err != nil {
		return m, err
	}

	if m.WriterWrites, err = registry.NewMetric("kafka.writer.write.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.WriterMessages, err = registry.NewMetric("kafka.writer.message.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.WriterBytes, err = registry.NewMetric("kafka.writer.message.bytes", metrics.Counter, metrics.Data); err != nil {
		return m, err
	}

	if m.WriterErrors, err = registry.NewMetric("kafka.writer.error.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.WriterWriteTime, err = registry.NewMetric("kafka.writer.write.seconds", metrics.Trend, metrics.Time); err != nil {
		return m, err
	}

	if m.WriterWaitTime, err = registry.NewMetric("kafka.writer.wait.seconds", metrics.Trend, metrics.Time); err != nil {
		return m, err
	}

	if m.WriterRetries, err = registry.NewMetric("kafka.writer.retries.count", metrics.Counter); err != nil {
		return m, err
	}

	if m.WriterBatchSize, err = registry.NewMetric("kafka.writer.batch.size", metrics.Counter); err != nil {
		return m, err
	}

	if m.WriterBatchBytes, err = registry.NewMetric("kafka.writer.batch.bytes", metrics.Counter, metrics.Data); err != nil {
		return m, err
	}

	if m.WriterMaxAttempts, err = registry.NewMetric("kafka.writer.attempts.max", metrics.Gauge); err != nil {
		return m, err
	}

	if m.WriterMaxBatchSize, err = registry.NewMetric("kafka.writer.batch.max", metrics.Gauge); err != nil {
		return m, err
	}

	if m.WriterBatchTimeout, err = registry.NewMetric("kafka.writer.batch.timeout", metrics.Gauge, metrics.Time); err != nil {
		return m, err
	}

	if m.WriterReadTimeout, err = registry.NewMetric("kafka.writer.read.timeout", metrics.Gauge, metrics.Time); err != nil {
		return m, err
	}

	if m.WriterWriteTimeout, err = registry.NewMetric("kafka.writer.write.timeout", metrics.Gauge, metrics.Time); err != nil {
		return m, err
	}

	if m.WriterRequiredAcks, err = registry.NewMetric("kafka.writer.acks.required", metrics.Gauge); err != nil {
		return m, err
	}

	if m.WriterAsync, err = registry.NewMetric("kafka.writer.async", metrics.Rate); err != nil {
		return m, err
	}

	return m, nil
}
