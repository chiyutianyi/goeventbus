package queue

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
)

type memoryQueue struct {
	queueMap map[string]chan string
}

func NewMemMq() (MessageQueue, error) {
	return &memoryQueue{queueMap: map[string]chan string{}}, nil
}

func (e *memoryQueue) Publish(ctx context.Context, topic, msg string) error {
	var (
		queue chan string
		ok    bool
	)
	if queue, ok = e.queueMap[topic]; !ok {
		queue = make(chan string)
		e.queueMap[topic] = queue
	}
	go func() { queue <- msg }()
	return nil
}

func (e *memoryQueue) Subscribe(ctx context.Context, topics []string, handle func(topic, msg string) error) {
	wg := sync.WaitGroup{}
	wg.Add(len(topics))
	for _, topic := range topics {
		go func(topic string) {
			defer wg.Done()
			for {
				msg := <-e.queueMap[topic]
				if err := handle(topic, msg); err != nil {
					log.Errorf("handle message: %s", err)
				}
			}
		}(topic)
	}
	wg.Wait()
}

func init() {
	Register("mem", NewMemMq)
}
