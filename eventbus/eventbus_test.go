package eventbus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chiyutianyi/goeventbus/eventbus"
	"github.com/chiyutianyi/goeventbus/queue"
	"github.com/stretchr/testify/assert"
)

type TestEvent struct {
	eventbus.DefaultEvent

	Data string `json:"data"`
}

func NewTestEvent(topic string, producer string, data string) *TestEvent {
	return &TestEvent{
		DefaultEvent: eventbus.NewDefaultEvent(topic, producer),
		Data:         data,
	}
}

func (e *TestEvent) GetData() interface{} {
	return e.Data
}

type TestSubscriber struct {
}

func TestEventbus(t *testing.T) {
	mq, err := queue.NewMemMq()
	assert.NoError(t, err)

	err = eventbus.Initialize(mq, 5)
	assert.NoError(t, err)

	go eventbus.Serve()

	ctx := context.Background()
	wg := sync.WaitGroup{}

	var count int32

	testcases := []eventbus.Event{
		NewTestEvent("topic1", "producer1", "msg1"),
		NewTestEvent("topic2", "producer2", "msg2"),
	}

	wg.Add(len(testcases))

	for _, tt := range testcases {
		func(testcase eventbus.Event) {
			eventbus.Register(testcase.GetTopic(), &eventbus.Subscriber{
				NewEvent: func() eventbus.Event { return &TestEvent{} },
				HandleEvent: func(event eventbus.Event) error {
					atomic.AddInt32(&count, 1)
					defer wg.Done()
					assert.Equal(t, testcase.GetData(), (event.(*TestEvent)).GetData())
					return nil
				},
			})
		}(tt)
	}

	for _, testcase := range testcases {
		eventbus.Post(ctx, testcase)
	}

	wg.Wait()

	assert.Equal(t, int32(len(testcases)), count)
}
