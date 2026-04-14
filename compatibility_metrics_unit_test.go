package kafka

import (
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
	assert.True(t, shouldRetryConsumerCompatibilityRead(true, 0, true))
	assert.False(t, shouldRetryConsumerCompatibilityRead(false, 0, true))
	assert.False(t, shouldRetryConsumerCompatibilityRead(true, 1, true))
	assert.False(t, shouldRetryConsumerCompatibilityRead(true, 0, false))
}
