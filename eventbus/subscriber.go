package eventbus

type Subscriber struct {
	// NewEvent is a function that creates a new event from a message.
	NewEvent func() Event

	// HandleEvent handles the event
	HandleEvent func(event Event) error
}
