package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompatibilityProducerMessageBytesIncludesTopic(t *testing.T) {
	msg := Message{
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}

	assert.Equal(t, 33, compatibilityProducerMessageBytes(strings.Repeat("t", 23), msg))
}

func TestCompatibilityConsumerFetchesAddsTrailingFetch(t *testing.T) {
	assert.Equal(t, 2.0, compatibilityConsumerFetches(1, 1, nil))
	assert.Equal(t, 2.0, compatibilityConsumerFetches(1, 1, errors.New("timeout")))
	assert.Equal(t, 1.0, compatibilityConsumerFetches(0, 1, errors.New("timeout")))
	assert.Equal(t, 0.0, compatibilityConsumerFetches(0, 1, nil))
}

func TestShouldRetryConsumerCompatibilityRead(t *testing.T) {
	parentCtx := context.Background()
	timeoutCtx, cancelTimeout := context.WithCancel(parentCtx)
	cancelTimeout()

	assert.True(t, shouldRetryConsumerCompatibilityRead(
		parentCtx,
		timeoutCtx,
		context.DeadlineExceeded,
		0,
		true,
	))

	activeParent, cancelParent := context.WithCancel(context.Background())
	canceledChild, cancelChild := context.WithCancel(activeParent)
	cancelChild()

	assert.True(t, shouldRetryConsumerCompatibilityRead(
		activeParent,
		canceledChild,
		context.Canceled,
		0,
		true,
	))

	cancelParent()

	assert.False(t, shouldRetryConsumerCompatibilityRead(
		activeParent,
		canceledChild,
		context.Canceled,
		0,
		true,
	))
	assert.False(t, shouldRetryConsumerCompatibilityRead(
		parentCtx,
		context.Background(),
		nil,
		0,
		true,
	))
	assert.False(t, shouldRetryConsumerCompatibilityRead(
		parentCtx,
		context.Background(),
		context.DeadlineExceeded,
		1,
		true,
	))
	assert.False(t, shouldRetryConsumerCompatibilityRead(
		parentCtx,
		context.Background(),
		context.DeadlineExceeded,
		0,
		false,
	))
}
