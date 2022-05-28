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

func registerMetrics(vu modules.VU) (kafkaMetrics, error) {
	var err error
	registry := vu.InitEnv().Registry
	m := kafkaMetrics{}

	m.ReaderDials, err = registry.NewMetric("kafka.reader.dial.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.ReaderFetches, err = registry.NewMetric("kafka.reader.fetches.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.ReaderMessages, err = registry.NewMetric("kafka.reader.message.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.ReaderBytes, err = registry.NewMetric("kafka.reader.message.bytes", metrics.Counter, metrics.Data)
	if err != nil {
		return m, err
	}

	m.ReaderRebalances, err = registry.NewMetric("kafka.reader.rebalance.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.ReaderTimeouts, err = registry.NewMetric("kafka.reader.timeouts.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.ReaderErrors, err = registry.NewMetric("kafka.reader.error.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.ReaderDialTime, err = registry.NewMetric("kafka.reader.dial.seconds", metrics.Trend, metrics.Time)
	if err != nil {
		return m, err
	}

	m.ReaderReadTime, err = registry.NewMetric("kafka.reader.read.seconds", metrics.Trend, metrics.Time)
	if err != nil {
		return m, err
	}

	m.ReaderWaitTime, err = registry.NewMetric("kafka.reader.wait.seconds", metrics.Trend, metrics.Time)
	if err != nil {
		return m, err
	}

	m.ReaderFetchSize, err = registry.NewMetric("kafka.reader.fetch.size", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.ReaderFetchBytes, err = registry.NewMetric("kafka.reader.fetch.bytes", metrics.Counter, metrics.Data)
	if err != nil {
		return m, err
	}

	m.ReaderOffset, err = registry.NewMetric("kafka.reader.offset", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.ReaderLag, err = registry.NewMetric("kafka.reader.lag", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.ReaderMinBytes, err = registry.NewMetric("kafka.reader.fetch_bytes.min", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.ReaderMaxBytes, err = registry.NewMetric("kafka.reader.fetch_bytes.max", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.ReaderMaxWait, err = registry.NewMetric("kafka.reader.fetch_wait.max", metrics.Gauge, metrics.Time)
	if err != nil {
		return m, err
	}

	m.ReaderQueueLength, err = registry.NewMetric("kafka.reader.queue.length", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.ReaderQueueCapacity, err = registry.NewMetric("kafka.reader.queue.capacity", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.WriterWrites, err = registry.NewMetric("kafka.writer.write.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.WriterMessages, err = registry.NewMetric("kafka.writer.message.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.WriterBytes, err = registry.NewMetric("kafka.writer.message.bytes", metrics.Counter, metrics.Data)
	if err != nil {
		return m, err
	}

	m.WriterErrors, err = registry.NewMetric("kafka.writer.error.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.WriterWriteTime, err = registry.NewMetric("kafka.writer.write.seconds", metrics.Trend, metrics.Time)
	if err != nil {
		return m, err
	}

	m.WriterWaitTime, err = registry.NewMetric("kafka.writer.wait.seconds", metrics.Trend, metrics.Time)
	if err != nil {
		return m, err
	}

	m.WriterRetries, err = registry.NewMetric("kafka.writer.retries.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.WriterBatchSize, err = registry.NewMetric("kafka.writer.batch.size", metrics.Counter)
	if err != nil {
		return m, err
	}

	m.WriterBatchBytes, err = registry.NewMetric("kafka.writer.batch.bytes", metrics.Counter, metrics.Data)
	if err != nil {
		return m, err
	}

	m.WriterMaxAttempts, err = registry.NewMetric("kafka.writer.attempts.max", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.WriterMaxBatchSize, err = registry.NewMetric("kafka.writer.batch.max", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.WriterBatchTimeout, err = registry.NewMetric("kafka.writer.batch.timeout", metrics.Gauge, metrics.Time)
	if err != nil {
		return m, err
	}

	m.WriterReadTimeout, err = registry.NewMetric("kafka.writer.read.timeout", metrics.Gauge, metrics.Time)
	if err != nil {
		return m, err
	}

	m.WriterWriteTimeout, err = registry.NewMetric("kafka.writer.write.timeout", metrics.Gauge, metrics.Time)
	if err != nil {
		return m, err
	}

	m.WriterRequiredAcks, err = registry.NewMetric("kafka.writer.acks.required", metrics.Gauge)
	if err != nil {
		return m, err
	}

	m.WriterAsync, err = registry.NewMetric("kafka.writer.async", metrics.Rate)
	if err != nil {
		return m, err
	}

	return m, nil
}
