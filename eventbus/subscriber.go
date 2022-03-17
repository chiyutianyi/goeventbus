package eventbus

type Subscriber struct {
	// NewEvent is a function that creates a new event from a message.
	NewEvent func() Event

	// Handler handles the event
	Handler func(event Event) error
}
