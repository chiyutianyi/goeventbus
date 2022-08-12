package queue_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chiyutianyi/goeventbus/queue"
	"github.com/stretchr/testify/assert"
)

func TestMemQueue(t *testing.T) {
	mq, err := queue.NewMemMq()
	assert.NoError(t, err)
	testQueue(t, mq)
}

func testQueue(t *testing.T, queue queue.MessageQueue) {
	ctx := context.Background()
	wg := sync.WaitGroup{}

	var count int32

	testcases := []struct {
		topic string
		msg   string
	}{
		{
			topic: "topic1",
			msg:   "msg1",
		}, {
			topic: "topic2",
			msg:   "msg2",
		},
	}

	wg.Add(len(testcases))

	topics := []string{"topic1", "topic2"}

	queue.Subscribe(ctx, topics, func(topic, msg string) error {
		defer wg.Done()
		atomic.AddInt32(&count, 1)
		switch topic {
		case "topic1":
			assert.Equal(t, "msg1", msg)
		case "topic2":
			assert.Equal(t, "msg2", msg)
		}
		return nil
	})

	go queue.Publish(ctx, "topic1", "msg1")
	go queue.Publish(ctx, "topic2", "msg2")

	wg.Wait()

	assert.Equal(t, int32(len(testcases)), count)
}
