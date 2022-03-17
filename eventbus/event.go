package eventbus

// Event
type Event interface {
	GetTopic() string

	GetProducer() string

	GetData() interface{}
}

type DefaultEvent struct {
	Topic string `json:"topic"`

	Producer string `json:"producer"`
}

func NewDefaultEvent(topic string, producer string) DefaultEvent {
	return DefaultEvent{Topic: topic, Producer: producer}
}

func (e *DefaultEvent) GetTopic() string {
	return e.Topic
}

func (e *DefaultEvent) GetProducer() string {
	return e.Producer
}
