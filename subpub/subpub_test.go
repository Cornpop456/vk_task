package subpub

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEventBusSubscribe(t *testing.T) {
	eb := NewEventBus()

	subscription, err := eb.Subscribe("test-subject", func(msg interface{}) {})

	assert.NoError(t, err, "Subscribe should not return an error")
	assert.NotNil(t, subscription, "Subscription should not be nil")

	_, ok := eb.subjects["test-subject"]
	assert.True(t, ok, "Subject should exist after subscription")
	assert.Len(t, eb.subjects["test-subject"], 1, "Expected 1 subscription")
}

func TestEventBusUnsubscribe(t *testing.T) {
	eb := NewEventBus()

	subscription, _ := eb.Subscribe("test-subject", func(msg interface{}) {})
	subscription.Unsubscribe()

	assert.Len(t, eb.subjects["test-subject"], 0, "Subscription should be removed after unsubscribe")
}

func TestEventBusPublish(t *testing.T) {
	eb := NewEventBus()
	receivedMsg := make(chan interface{}, 1)

	_, _ = eb.Subscribe("test-subject", func(msg interface{}) {
		receivedMsg <- msg
	})

	testMessage := "hello world"
	err := eb.Publish("test-subject", testMessage)
	assert.NoError(t, err, "Publish should not return an error")

	select {
	case msg := <-receivedMsg:
		assert.Equal(t, testMessage, msg, "Received message should match published message")
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Timed out waiting for message")
	}
}

func TestEventBusPublishMultipleSubscribers(t *testing.T) {
	eb := NewEventBus()
	var wg sync.WaitGroup
	mutex := sync.Mutex{}
	receivedCount := 0

	numSubscribers := 5
	wg.Add(numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		_, _ = eb.Subscribe("test-subject", func(msg interface{}) {
			mutex.Lock()
			receivedCount++
			mutex.Unlock()
			wg.Done()
		})
	}

	err := eb.Publish("test-subject", "test message")
	assert.NoError(t, err, "Publish should not return an error")

	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-c:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Timed out waiting for all handlers")
	}

	assert.Equal(t, numSubscribers, receivedCount, "All subscribers should receive the message")
}

func TestEventBusPublishNoSubscribers(t *testing.T) {
	eb := NewEventBus()

	err := eb.Publish("non-existent-subject", "test message")
	assert.NoError(t, err, "Should not error when publishing to a topic with no subscribers")
}

func TestEventBusMultipleTopics(t *testing.T) {
	eb := NewEventBus()
	receivedTopic1 := false
	receivedTopic2 := false

	_, _ = eb.Subscribe("topic1", func(msg interface{}) {
		receivedTopic1 = true
	})

	_, _ = eb.Subscribe("topic2", func(msg interface{}) {
		receivedTopic2 = true
	})

	_ = eb.Publish("topic1", "test message")

	time.Sleep(10 * time.Millisecond)

	assert.True(t, receivedTopic1, "Topic1 subscriber should have been called")
	assert.False(t, receivedTopic2, "Topic2 subscriber should not have been called")
}

func TestEventBusClose(t *testing.T) {
	eb := NewEventBus()

	_, _ = eb.Subscribe("topic1", func(msg interface{}) {})
	_, _ = eb.Subscribe("topic2", func(msg interface{}) {})

	ctx := context.Background()
	err := eb.Close(ctx)
	assert.NoError(t, err, "Close should not return an error")
	assert.Empty(t, eb.subjects, "All subjects should be cleared after closing")
	assert.Equal(t, 0, eb.lastSubIdx, "lastSubIdx should be reset to 0")
}

func TestEventBusCloseWithCanceledContext(t *testing.T) {
	eb := NewEventBus()

	_, _ = eb.Subscribe("topic1", func(msg interface{}) {})
	_, _ = eb.Subscribe("topic2", func(msg interface{}) {})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.False(t, eb.closed, "Bus should not be closed before calling Close")
	assert.NotEmpty(t, eb.subjects, "Subjects should exist before closing")

	err := eb.Close(ctx)

	assert.Error(t, err, "Close should return an error when context is canceled")
	assert.Equal(t, context.Canceled, err, "Error should be context.Canceled")

	assert.True(t, eb.closed, "Bus should be marked as closed even with canceled context")
	assert.Empty(t, eb.subjects, "Subjects should be cleared when context is canceled")

	pubErr := eb.Publish("topic1", "test message")
	assert.Error(t, pubErr, "Publish should return an error after Close")
	assert.Equal(t, ErrBusClosed, pubErr, "Publish error should be ErrBusClosed")
}
func TestEventBusSlowHandlerDoesntBlockFastHandlers(t *testing.T) {
	eb := NewEventBus()

	var completionOrder []string
	orderMutex := sync.Mutex{}

	recordCompletion := func(name string) {
		orderMutex.Lock()
		defer orderMutex.Unlock()
		completionOrder = append(completionOrder, name)
	}

	_, err := eb.Subscribe("test-topic", func(msg interface{}) {
		time.Sleep(100 * time.Millisecond) // Simulate long processing
		recordCompletion("slow-handler")
	})
	assert.NoError(t, err)

	numFastHandlers := 3
	for i := 0; i < numFastHandlers; i++ {
		handlerName := fmt.Sprintf("fast-handler-%d", i)
		_, err := eb.Subscribe("test-topic", func(msg interface{}) {
			recordCompletion(handlerName)
		})
		assert.NoError(t, err)
	}

	err = eb.Publish("test-topic", "test-message")
	assert.NoError(t, err)

	time.Sleep(150 * time.Millisecond)

	orderMutex.Lock()
	defer orderMutex.Unlock()

	assert.Equal(t, numFastHandlers+1, len(completionOrder), "All handlers should have completed")

	assert.Equal(t, "slow-handler", completionOrder[len(completionOrder)-1],
		"Slow handler should complete last")

	for i := 0; i < numFastHandlers; i++ {
		assert.Contains(t, completionOrder[:numFastHandlers], fmt.Sprintf("fast-handler-%d", i),
			"Fast handler should complete before slow handler")
	}
}
