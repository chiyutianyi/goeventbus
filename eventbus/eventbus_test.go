package eventbus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestEventbus(t *testing.T) {
	var (
		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		wg          = sync.WaitGroup{}
		count       int32
	)
	defer cancel()

	mq, err := queue.NewMemMq()
	assert.NoError(t, err)

	eb := eventbus.NewEventbus(mq, 5)
	assert.NoError(t, err)

	testcases := []eventbus.Event{
		NewTestEvent("topic1", "producer1", "msg1"),
		NewTestEvent("topic2", "producer2", "msg2"),
	}

	wg.Add(len(testcases) * 2)

	for _, tt := range testcases {
		func(testcase eventbus.Event) {
			assert.NoError(t, eb.Register(testcase.GetTopic(),
				&eventbus.Subscriber{
					UID:      func() string { return "subscriber1" },
					NewEvent: func() eventbus.Event { return &TestEvent{} },
					HandleEvent: func(ctx context.Context, event eventbus.Event) error {
						atomic.AddInt32(&count, 1)
						defer wg.Done()
						assert.Equal(t, testcase.GetData(), (event.(*TestEvent)).GetData())
						assert.Equal(t, testcase.GetProducer(), (event.(*TestEvent)).GetProducer())
						return nil
					},
				},
				&eventbus.Subscriber{
					UID:      func() string { return "subscriber2" },
					NewEvent: func() eventbus.Event { return &TestEvent{} },
					HandleEvent: func(ctx context.Context, event eventbus.Event) error {
						atomic.AddInt32(&count, 1)
						defer wg.Done()
						assert.Equal(t, testcase.GetData(), (event.(*TestEvent)).GetData())
						assert.Equal(t, testcase.GetProducer(), (event.(*TestEvent)).GetProducer())
						return nil
					},
				}))
		}(tt)
	}

	eb.Serve(ctx)

	for _, testcase := range testcases {
		assert.NoError(t, eb.Post(ctx, testcase))
	}

	wg.Wait()

	assert.EqualValues(t, len(testcases)*2, count)
}
