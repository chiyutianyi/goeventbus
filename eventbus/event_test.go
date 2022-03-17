package eventbus_test

import (
	"github.com/chiyutianyi/goeventbus/eventbus"
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
