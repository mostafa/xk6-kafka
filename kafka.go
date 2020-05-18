package main

import (
	"context"
	"time"

	"github.com/loadimpact/k6/lib"
	"github.com/loadimpact/k6/stats"
	kafkago "github.com/segmentio/kafka-go"
)

var (
	Dials      = stats.New("kafka.writer.dial.count", stats.Counter)
	Writes     = stats.New("kafka.writer.write.count", stats.Counter)
	Messages   = stats.New("kafka.writer.message.count", stats.Counter)
	Bytes      = stats.New("kafka.writer.message.bytes", stats.Counter, stats.Data)
	Rebalances = stats.New("kafka.writer.rebalance.count", stats.Counter)
	Errors     = stats.New("kafka.writer.error.count", stats.Counter)

	DialTime   = stats.New("kafka.writer.dial.seconds", stats.Trend, stats.Time)
	WriteTime  = stats.New("kafka.writer.write.seconds", stats.Trend, stats.Time)
	WaitTime   = stats.New("kafka.writer.wait.seconds", stats.Trend, stats.Time)
	Retries    = stats.New("kafka.writer.retries.count", stats.Counter)
	BatchSize  = stats.New("kafka.writer.batch.size", stats.Counter)
	BatchBytes = stats.New("kafka.writer.batch.bytes", stats.Counter, stats.Data)

	MaxAttempts       = stats.New("kafka.writer.attempts.max", stats.Gauge)
	MaxBatchSize      = stats.New("kafka.writer.batch.max", stats.Gauge)
	BatchTimeout      = stats.New("kafka.writer.batch.timeout", stats.Gauge, stats.Time)
	ReadTimeout       = stats.New("kafka.writer.read.timeout", stats.Gauge, stats.Time)
	WriteTimeout      = stats.New("kafka.writer.write.timeout", stats.Gauge, stats.Time)
	RebalanceInterval = stats.New("kafka.writer.rebalance.interval", stats.Gauge, stats.Time)
	RequiredAcks      = stats.New("kafka.writer.acks.required", stats.Gauge)
	Async             = stats.New("kafka.writer.async", stats.Rate)
	QueueLength       = stats.New("kafka.writer.queue.length", stats.Gauge)
	QueueCapacity     = stats.New("kafka.writer.queue.capacity", stats.Gauge)
)

type kafka struct{}

func New() *kafka {
	return &kafka{}
}

func (*kafka) Connect(brokers []string, topic string) *kafkago.Writer {
	return kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	})
}

func (*kafka) Produce(ctx context.Context, writer *kafkago.Writer, messages []map[string]string) error {
	state := lib.GetState(ctx)
	// fmt.Print(isChanClosed(state.Samples))
	// fmt.Println(state.Samples)
	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {
		kafkaMessages[i] = kafkago.Message{
			Key:   []byte(message["key"]),
			Value: []byte(message["value"]),
		}
	}

	err := writer.WriteMessages(ctx, kafkaMessages...)
	currentStats := writer.Stats()

	tags := make(map[string]string)
	tags["clientid"] = currentStats.ClientID
	tags["topic"] = currentStats.Topic

	dialsSample := stats.Sample{
		Time:   time.Now(),
		Metric: Dials,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Dials),
	}

	stats.PushIfNotDone(ctx, state.Samples, dialsSample)
	// {
	// 	Time:   now,
	// 	Metric: Writes,
	// 	Value:  float64(currentStats.Writes),
	// },

	// return error
	return err
}

func (*kafka) Close(writer *kafkago.Writer) {
	writer.Close()
}
