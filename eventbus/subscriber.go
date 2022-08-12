package eventbus

import "context"

// Subscriber is the interface for subscriber
type Subscriber struct {
	// UID
	UID func() string

	// NewEvent is a function that creates a new event from a message.
	NewEvent func() Event

	// HandleEvent handles the event
	HandleEvent func(ctx context.Context, event Event) error
}
