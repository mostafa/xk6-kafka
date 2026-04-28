package kafka

import (
	"context"
	"testing"

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

	assert.NoError(t, a.Close())
}
