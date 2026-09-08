package kafka

import (
	"context"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminClientDeleteTopicEmptyName(t *testing.T) {
	t.Parallel()
	mockCluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	defer mockCluster.Close()

	ctx := context.Background()
	admin, err := NewAdminClientFromConnectionConfig(&ConnectionConfig{
		Address: mockCluster.BootstrapServers(),
	})
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	err = admin.DeleteTopic(ctx, "")
	require.Error(t, err)
}

func TestAdminClientNilReceiver(t *testing.T) {
	t.Parallel()
	var a *AdminClient
	ctx := context.Background()

	_, err := a.ListTopics(ctx)
	require.Error(t, err)

	_, err = a.GetMetadata(ctx, "t")
	require.Error(t, err)

	err = a.CreateTopic(ctx, TopicConfig{Topic: "x"})
	require.Error(t, err)

	err = a.DeleteTopic(ctx, "x")
	require.Error(t, err)

	_, err = a.InitializeConsumerGroupOffsets(ctx, ConsumerGroupOffsetsConfig{
		GroupID: "group",
		Topics:  []string{"topic"},
	})
	require.Error(t, err)

	assert.NoError(t, a.Close())
}

func TestNormalizedTopics(t *testing.T) {
	t.Parallel()

	topics, err := normalizedTopics([]string{" topic-b ", "topic-a", "topic-b"})
	require.NoError(t, err)
	assert.Equal(t, []string{"topic-a", "topic-b"}, topics)

	_, err = normalizedTopics(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTopicsMustNotBeEmpty)

	_, err = normalizedTopics([]string{"topic", " "})
	require.Error(t, err)
	assert.ErrorIs(t, err, errTopicMustNotBeEmpty)
}

func TestConsumerGroupOffsetSnapshot(t *testing.T) {
	t.Parallel()

	topicA := "topic-a"
	topicB := "topic-b"
	requests := map[ckafka.TopicPartition]ckafka.OffsetSpec{
		{Topic: &topicB, Partition: 2}: ckafka.LatestOffsetSpec,
		{Topic: &topicA, Partition: 1}: ckafka.LatestOffsetSpec,
		{Topic: &topicA, Partition: 0}: ckafka.LatestOffsetSpec,
	}
	resultTopicA := "topic-a"
	resultTopicB := "topic-b"

	snapshot, err := consumerGroupOffsetSnapshot(requests, ckafka.ListOffsetsResult{
		ResultInfos: map[ckafka.TopicPartition]ckafka.ListOffsetsResultInfo{
			{Topic: &resultTopicB, Partition: 2}: {Offset: 23},
			{Topic: &resultTopicA, Partition: 0}: {Offset: 7},
			{Topic: &resultTopicA, Partition: 1}: {Offset: 11},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []ConsumerGroupOffset{
		{Topic: "topic-a", Partition: 0, Offset: 7},
		{Topic: "topic-a", Partition: 1, Offset: 11},
		{Topic: "topic-b", Partition: 2, Offset: 23},
	}, snapshot)
}

func TestConsumerGroupOffsetSnapshotRejectsMissingPartition(t *testing.T) {
	t.Parallel()

	topic := "topic"
	_, err := consumerGroupOffsetSnapshot(
		map[ckafka.TopicPartition]ckafka.OffsetSpec{
			{Topic: &topic, Partition: 0}: ckafka.LatestOffsetSpec,
		},
		ckafka.ListOffsetsResult{ResultInfos: map[ckafka.TopicPartition]ckafka.ListOffsetsResultInfo{}},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing partitions: topic[0]")
}

func TestValidateAlteredConsumerGroupOffsets(t *testing.T) {
	t.Parallel()

	topic := "topic"
	expected := []ConsumerGroupOffset{{Topic: topic, Partition: 0, Offset: 12}}

	err := validateAlteredConsumerGroupOffsets("group", expected, ckafka.AlterConsumerGroupOffsetsResult{
		ConsumerGroupsTopicPartitions: []ckafka.ConsumerGroupTopicPartitions{{
			Group: "group",
			Partitions: []ckafka.TopicPartition{{
				Topic:     &topic,
				Partition: 0,
				Offset:    12,
			}},
		}},
	})
	require.NoError(t, err)

	err = validateAlteredConsumerGroupOffsets("group", expected, ckafka.AlterConsumerGroupOffsetsResult{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 1 group, got 0")

	err = validateAlteredConsumerGroupOffsets("group", expected, ckafka.AlterConsumerGroupOffsetsResult{
		ConsumerGroupsTopicPartitions: []ckafka.ConsumerGroupTopicPartitions{{Group: "other"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `expected group "group", got "other"`)

	err = validateAlteredConsumerGroupOffsets("group", expected, ckafka.AlterConsumerGroupOffsetsResult{
		ConsumerGroupsTopicPartitions: []ckafka.ConsumerGroupTopicPartitions{{Group: "group"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing partition: topic[0]")

	err = validateAlteredConsumerGroupOffsets("group", expected, ckafka.AlterConsumerGroupOffsetsResult{
		ConsumerGroupsTopicPartitions: []ckafka.ConsumerGroupTopicPartitions{{
			Group: "group",
			Partitions: []ckafka.TopicPartition{{
				Topic:     &topic,
				Partition: 0,
				Offset:    99,
			}},
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partition topic[0]: expected offset 12, got 99")
}

func TestInitializeConsumerGroupOffsetsWithMockCluster(t *testing.T) {
	t.Parallel()

	mockCluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	defer mockCluster.Close()

	const (
		topicName = "initialize-group-offsets-topic"
		groupID   = "initialize-group-offsets-group"
	)
	require.NoError(t, mockCluster.CreateTopic(topicName, 2, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	producer, err := NewProducerFromWriterConfig(&WriterConfig{
		Brokers: []string{mockCluster.BootstrapServers()},
		Topic:   topicName,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()
	require.NoError(t, producer.Produce(ctx, []Message{
		{Value: []byte("historical-0"), Partition: 0},
		{Value: []byte("historical-1"), Partition: 1},
	}))
	require.NoError(t, producer.Flush(ctx))

	admin, err := NewAdminClientFromConnectionConfig(&ConnectionConfig{
		Address: mockCluster.BootstrapServers(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()

	snapshot, err := admin.InitializeConsumerGroupOffsets(ctx, ConsumerGroupOffsetsConfig{
		GroupID: groupID,
		Topics:  []string{topicName},
	})
	require.NoError(t, err)
	require.Len(t, snapshot, 2)
	assert.Equal(t, int64(2), snapshot[0].Offset+snapshot[1].Offset)

	consumer, err := NewConsumerFromReaderConfig(&ReaderConfig{
		Brokers:     []string{mockCluster.BootstrapServers()},
		GroupID:     groupID,
		GroupTopics: []string{topicName},
		MaxWait:     Duration{Duration: 5 * time.Second},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, consumer.Close()) }()

	require.NoError(t, producer.Produce(ctx, []Message{{
		Value:     []byte("test-event"),
		Partition: 0,
	}}))
	require.NoError(t, producer.Flush(ctx))

	messages, err := consumer.Consume(ctx, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, []byte("test-event"), messages[0].Value)

	// The historical records must have been skipped: nothing else remains to
	// consume, so a short-deadline read comes back empty.
	emptyCtx, emptyCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer emptyCancel()
	extra, err := consumer.Consume(emptyCtx, 1)
	require.Error(t, err)
	assert.Empty(t, extra)
}

func TestAdminClientTracksProducerForLifecycle(t *testing.T) {
	t.Parallel()
	mockCluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	defer mockCluster.Close()

	admin, err := NewAdminClientFromConnectionConfig(&ConnectionConfig{
		Address: mockCluster.BootstrapServers(),
	})
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	require.NotNil(t, admin.client)
	require.NotNil(t, admin.pClient)
	require.NotNil(t, admin.doneChan)
}

func TestAdminClientCloseIsIdempotentAndClearsClients(t *testing.T) {
	t.Parallel()
	mockCluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	defer mockCluster.Close()

	admin, err := NewAdminClientFromConnectionConfig(&ConnectionConfig{
		Address: mockCluster.BootstrapServers(),
	})
	require.NoError(t, err)

	require.NoError(t, admin.Close())
	require.Nil(t, admin.client)
	require.Nil(t, admin.pClient)
	require.Nil(t, admin.doneChan)

	require.NoError(t, admin.Close())
}

func TestAdminClientOperationsFailAfterClose(t *testing.T) {
	t.Parallel()
	mockCluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	defer mockCluster.Close()

	admin, err := NewAdminClientFromConnectionConfig(&ConnectionConfig{
		Address: mockCluster.BootstrapServers(),
	})
	require.NoError(t, err)

	require.NoError(t, admin.Close())

	_, err = admin.ListTopics(context.Background())
	require.Error(t, err)

	_, err = admin.GetMetadata(context.Background(), "any-topic")
	require.Error(t, err)

	err = admin.CreateTopic(context.Background(), TopicConfig{Topic: "any-topic"})
	require.Error(t, err)

	err = admin.DeleteTopic(context.Background(), "any-topic")
	require.Error(t, err)

	_, err = admin.InitializeConsumerGroupOffsets(context.Background(), ConsumerGroupOffsetsConfig{
		GroupID: "group",
		Topics:  []string{"any-topic"},
	})
	require.Error(t, err)
}
