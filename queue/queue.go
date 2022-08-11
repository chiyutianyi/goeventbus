package queue

import (
	"context"
	"fmt"
	"sync"
)

// MessageQueue is the interface for message queue
type MessageQueue interface {
	// Publish publishes the event to all subscribers
	Publish(ctx context.Context, topic, msg string) error
	// Subscribe subscribes to the topic
	Subscribe(ctx context.Context, topics []string, handle func(topic, msg string) error)
}

type Creator func() (MessageQueue, error)

var (
	provides  = make(map[string]Creator)
	driversMu sync.RWMutex
)

func Register(name string, creator Creator) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if _, ok := provides[name]; ok {
		panic(fmt.Sprintf("queue %s already registered", name))
	}
	provides[name] = creator
}

// Init initializes the message queue
func Initialize(mqtype string) (MessageQueue, error) {
	if mqtype == "" {
		return nil, fmt.Errorf("message queue type is empty")
	}
	driversMu.RLock()
	defer driversMu.RUnlock()
	if creator, ok := provides[mqtype]; ok {
		return creator()
	}
	return nil, fmt.Errorf("message queue %s not supported", mqtype)
}
