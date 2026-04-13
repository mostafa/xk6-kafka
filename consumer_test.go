package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerContextErrorWrapsDeadlineExceeded(t *testing.T) {
	err := consumerContextError(context.DeadlineExceeded)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.EqualError(t, err, "Consumer context cancelled., OriginalError: context deadline exceeded")
}

func TestConsumerReadErrorWrapsOriginalError(t *testing.T) {
	originalErr := errors.New("broker disconnect")
	err := consumerReadError(originalErr)
	require.Error(t, err)
	assert.ErrorIs(t, err, originalErr)
	assert.EqualError(t, err, "Failed to consume message., OriginalError: broker disconnect")
}

func TestConsumerReadTimeoutNormalizationPrefersExpiredContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	time.Sleep(time.Millisecond)

	normalizedErr := normalizeConsumerReadError(ctx, errors.New("connection closed by peer"))
	require.Error(t, normalizedErr)
	assert.ErrorIs(t, normalizedErr, context.DeadlineExceeded)
}

func TestConsumerReadTimeoutNormalizationPreservesBrokerErrorWhileContextActive(t *testing.T) {
	originalErr := errors.New("connection closed by peer")
	normalizedErr := normalizeConsumerReadError(context.Background(), originalErr)
	require.Error(t, normalizedErr)
	assert.ErrorIs(t, normalizedErr, originalErr)
	assert.NotErrorIs(t, normalizedErr, context.DeadlineExceeded)
}

func TestConsumerCompatibilityTimeoutDetectsExpiredChildDeadline(t *testing.T) {
	parentCtx := context.Background()
	consumeCtx, cancel := context.WithTimeout(parentCtx, time.Nanosecond)
	defer cancel()

	time.Sleep(time.Millisecond)

	assert.True(t, isConsumerCompatibilityTimeout(
		parentCtx,
		consumeCtx,
		consumerContextError(context.Canceled),
	))
}

func TestConsumerCompatibilityTimeoutIgnoresExternalCancellation(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	consumeCtx, cancelConsume := context.WithTimeout(parentCtx, time.Second)
	defer cancelConsume()

	cancelParent()

	assert.False(t, isConsumerCompatibilityTimeout(
		parentCtx,
		consumeCtx,
		consumerContextError(context.Canceled),
	))
}
