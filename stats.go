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

type kafkaMetricDefinition struct {
	name         string
	metricType   metrics.MetricType
	valueType    metrics.ValueType
	hasValueType bool
	assign       func(*kafkaMetrics, *metrics.Metric)
}

var kafkaMetricDefinitions = []kafkaMetricDefinition{
	{name: "kafka_reader_dial_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderDials = metric }},
	{name: "kafka_reader_fetches_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderFetches = metric }},
	{name: "kafka_reader_message_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderMessages = metric }},
	{name: "kafka_reader_message_bytes", metricType: metrics.Counter, valueType: metrics.Data, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderBytes = metric }},
	{name: "kafka_reader_rebalance_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderRebalances = metric }},
	{name: "kafka_reader_timeouts_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderTimeouts = metric }},
	{name: "kafka_reader_error_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderErrors = metric }},
	{name: "kafka_reader_dial_seconds", metricType: metrics.Trend, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderDialTime = metric }},
	{name: "kafka_reader_read_seconds", metricType: metrics.Trend, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderReadTime = metric }},
	{name: "kafka_reader_wait_seconds", metricType: metrics.Trend, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderWaitTime = metric }},
	{name: "kafka_reader_fetch_size", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderFetchSize = metric }},
	{name: "kafka_reader_fetch_bytes", metricType: metrics.Counter, valueType: metrics.Data, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderFetchBytes = metric }},
	{name: "kafka_reader_offset", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderOffset = metric }},
	{name: "kafka_reader_lag", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderLag = metric }},
	{name: "kafka_reader_fetch_bytes_min", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderMinBytes = metric }},
	{name: "kafka_reader_fetch_bytes_max", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderMaxBytes = metric }},
	{name: "kafka_reader_fetch_wait_max", metricType: metrics.Gauge, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderMaxWait = metric }},
	{name: "kafka_reader_queue_length", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderQueueLength = metric }},
	{name: "kafka_reader_queue_capacity", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.ReaderQueueCapacity = metric }},
	{name: "kafka_writer_write_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterWrites = metric }},
	{name: "kafka_writer_message_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterMessages = metric }},
	{name: "kafka_writer_message_bytes", metricType: metrics.Counter, valueType: metrics.Data, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterBytes = metric }},
	{name: "kafka_writer_error_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterErrors = metric }},
	{name: "kafka_writer_batch_seconds", metricType: metrics.Trend, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterBatchTime = metric }},
	{name: "kafka_writer_batch_queue_seconds", metricType: metrics.Trend, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterBatchQueueTime = metric }},
	{name: "kafka_writer_write_seconds", metricType: metrics.Trend, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterWriteTime = metric }},
	{name: "kafka_writer_wait_seconds", metricType: metrics.Trend, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterWaitTime = metric }},
	{name: "kafka_writer_retries_count", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterRetries = metric }},
	{name: "kafka_writer_batch_size", metricType: metrics.Counter, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterBatchSize = metric }},
	{name: "kafka_writer_batch_bytes", metricType: metrics.Counter, valueType: metrics.Data, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterBatchBytes = metric }},
	{name: "kafka_writer_attempts_max", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterMaxAttempts = metric }},
	{name: "kafka_writer_batch_max", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterMaxBatchSize = metric }},
	{name: "kafka_writer_batch_timeout", metricType: metrics.Gauge, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterBatchTimeout = metric }},
	{name: "kafka_writer_read_timeout", metricType: metrics.Gauge, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterReadTimeout = metric }},
	{name: "kafka_writer_write_timeout", metricType: metrics.Gauge, valueType: metrics.Time, hasValueType: true, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterWriteTimeout = metric }},
	{name: "kafka_writer_acks_required", metricType: metrics.Gauge, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterRequiredAcks = metric }},
	{name: "kafka_writer_async", metricType: metrics.Rate, assign: func(km *kafkaMetrics, metric *metrics.Metric) { km.WriterAsync = metric }},
}

func registeredKafkaMetricNames() []string {
	names := make([]string, 0, len(kafkaMetricDefinitions))
	for _, definition := range kafkaMetricDefinitions {
		names = append(names, definition.name)
	}

	return names
}

func registerKafkaMetric(
	registry *metrics.Registry,
	definition kafkaMetricDefinition,
) (*metrics.Metric, error) {
	if definition.hasValueType {
		return registry.NewMetric(definition.name, definition.metricType, definition.valueType)
	}

	return registry.NewMetric(definition.name, definition.metricType)
}

// registerMetrics registers the metrics for the kafka module in the metrics registry.
func registerMetrics(vu modules.VU) (kafkaMetrics, error) {
	registry := vu.InitEnv().Registry
	registeredMetrics := kafkaMetrics{}

	for _, definition := range kafkaMetricDefinitions {
		metric, err := registerKafkaMetric(registry, definition)
		if err != nil {
			return registeredMetrics, err
		}

		definition.assign(&registeredMetrics, metric)
	}

	return registeredMetrics, nil
}
