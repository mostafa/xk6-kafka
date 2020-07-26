package main

import "github.com/loadimpact/k6/stats"

var (
	ReaderDials      = stats.New("kafka.reader.dial.count", stats.Counter)
	ReaderFetches    = stats.New("kafka.reader.fetches.count", stats.Counter)
	ReaderMessages   = stats.New("kafka.reader.message.count", stats.Counter)
	ReaderBytes      = stats.New("kafka.reader.message.bytes", stats.Counter, stats.Data)
	ReaderRebalances = stats.New("kafka.reader.rebalance.count", stats.Counter)
	ReaderTimeouts   = stats.New("kafka.reader.timeouts.count", stats.Counter)
	ReaderErrors     = stats.New("kafka.reader.error.count", stats.Counter)

	ReaderDialTime   = stats.New("kafka.reader.dial.seconds", stats.Trend, stats.Time)
	ReaderReadTime   = stats.New("kafka.reader.read.seconds", stats.Trend, stats.Time)
	ReaderWaitTime   = stats.New("kafka.reader.wait.seconds", stats.Trend, stats.Time)
	ReaderFetchSize  = stats.New("kafka.reader.fetch.size", stats.Counter)
	ReaderFetchBytes = stats.New("kafka.reader.fetch.bytes", stats.Counter, stats.Data)

	ReaderOffset        = stats.New("kafka.reader.offset", stats.Gauge)
	ReaderLag           = stats.New("kafka.reader.lag", stats.Gauge)
	ReaderMinBytes      = stats.New("kafka.reader.fetch_bytes.min", stats.Gauge)
	ReaderMaxBytes      = stats.New("kafka.reader.fetch_bytes.max", stats.Gauge)
	ReaderMaxWait       = stats.New("kafka.reader.fetch_wait.max", stats.Gauge, stats.Time)
	ReaderQueueLength   = stats.New("kafka.reader.queue.length", stats.Gauge)
	ReaderQueueCapacity = stats.New("kafka.reader.queue.capacity", stats.Gauge)

	WriterDials      = stats.New("kafka.writer.dial.count", stats.Counter)
	WriterWrites     = stats.New("kafka.writer.write.count", stats.Counter)
	WriterMessages   = stats.New("kafka.writer.message.count", stats.Counter)
	WriterBytes      = stats.New("kafka.writer.message.bytes", stats.Counter, stats.Data)
	WriterRebalances = stats.New("kafka.writer.rebalance.count", stats.Counter)
	WriterErrors     = stats.New("kafka.writer.error.count", stats.Counter)

	WriterDialTime   = stats.New("kafka.writer.dial.seconds", stats.Trend, stats.Time)
	WriterWriteTime  = stats.New("kafka.writer.write.seconds", stats.Trend, stats.Time)
	WriterWaitTime   = stats.New("kafka.writer.wait.seconds", stats.Trend, stats.Time)
	WriterRetries    = stats.New("kafka.writer.retries.count", stats.Counter)
	WriterBatchSize  = stats.New("kafka.writer.batch.size", stats.Counter)
	WriterBatchBytes = stats.New("kafka.writer.batch.bytes", stats.Counter, stats.Data)

	WriterMaxAttempts       = stats.New("kafka.writer.attempts.max", stats.Gauge)
	WriterMaxBatchSize      = stats.New("kafka.writer.batch.max", stats.Gauge)
	WriterBatchTimeout      = stats.New("kafka.writer.batch.timeout", stats.Gauge, stats.Time)
	WriterReadTimeout       = stats.New("kafka.writer.read.timeout", stats.Gauge, stats.Time)
	WriterWriteTimeout      = stats.New("kafka.writer.write.timeout", stats.Gauge, stats.Time)
	WriterRebalanceInterval = stats.New("kafka.writer.rebalance.interval", stats.Gauge, stats.Time)
	WriterRequiredAcks      = stats.New("kafka.writer.acks.required", stats.Gauge)
	WriterAsync             = stats.New("kafka.writer.async", stats.Rate)
	WriterQueueLength       = stats.New("kafka.writer.queue.length", stats.Gauge)
	WriterQueueCapacity     = stats.New("kafka.writer.queue.capacity", stats.Gauge)
)
