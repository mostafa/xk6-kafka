package main

import (
	"context"
	"errors"
	"reflect"
	"time"
	"unsafe"

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

type ctxKey int

const (
	ctxKeyState ctxKey = iota
)

func GetState(ctx interface{}) (*lib.State, error) {
	// FIXME: This function is a hack to get lib.State from the context

	contextValues := reflect.ValueOf(ctx).Elem()
	contextKeys := reflect.TypeOf(ctx).Elem()

	if contextKeys.Kind() == reflect.Struct {
		for i := 0; i < contextValues.NumField(); i++ {
			if i != 2 {
				// Hack to get past a value in context
				continue
			}
			reflectValue := contextValues.Field(i)
			reflectValue = reflect.NewAt(reflectValue.Type(), unsafe.Pointer(reflectValue.UnsafeAddr())).Elem()
			reflectField := contextKeys.Field(i)

			if reflectField.Name != "Context" {
				return reflectValue.Interface().(*lib.State), nil
			}
		}
	}

	return nil, errors.New("State is nil")
}

func (*kafka) Produce(ctx context.Context, writer *kafkago.Writer, messages []map[string]string) error {
	state, err := GetState(ctx)

	if err == nil {
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

		now := time.Now()

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Dials,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Dials),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Writes,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Writes),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Messages,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Messages),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Bytes,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Bytes),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Rebalances,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Rebalances),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Errors,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Errors),
		})

		// values := map[string]float64{
		// 	"kafka.writer.dial.seconds.avg": currentStats.DialTime.Avg,
		// 	"kafka.writer.dial.seconds.min": currentStats.DialTime.Min,
		// 	"kafka.writer.dial.seconds.max": currentStats.DialTime.Max,
		// }

		// stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		// 	Type:   cloud.DataTypeMap,
		// 	Metric: DialTime,
		// 	Data: &cloud.SampleDataMap{
		// 		Time:   now,
		// 		Tags:   stats.IntoSampleTags(&tags),
		// 		Values: values
		// 	}
		// })

		return err
	}

	return errors.New("State is nil")
}

func (*kafka) Close(writer *kafkago.Writer) {
	writer.Close()
}
