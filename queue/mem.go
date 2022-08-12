package queue

import (
	"context"
	"sync"

	logs "github.com/sirupsen/logrus"
)

type memoryQueue struct {
	sync.RWMutex
	queueMap map[string]chan string
}

func NewMemMq() (MessageQueue, error) {
	return &memoryQueue{queueMap: map[string]chan string{}}, nil
}

func (e *memoryQueue) Publish(ctx context.Context, topic, msg string) error {
	var (
		q  chan string
		ok bool
	)
	e.RLock()
	if q, ok = e.queueMap[topic]; !ok {
		e.RUnlock()
		e.Lock()
		q = make(chan string)
		e.queueMap[topic] = q
		e.Unlock()
	} else {
		e.RUnlock()
	}
	q <- msg
	return nil
}

func (e *memoryQueue) Subscribe(ctx context.Context, topics []string, handle func(topic, msg string) error) {
	wg := sync.WaitGroup{}
	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			wg.Done()
			for {
				e.Lock()
				q, ok := e.queueMap[topic]
				if !ok {
					e.Unlock()
					continue
				}
				e.Unlock()

				select {
				case msg := <-q:
					func() {
						defer func() {
							if err := recover(); err != nil {
								logs.Errorf("handle message panic: %s", err)
							}
						}()
						if err := handle(topic, msg); err != nil {
							logs.Errorf("handle message: %s", err)
						}
					}()
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
		}(topic)
	}
	wg.Wait()
}

func init() {
	Register("mem", NewMemMq)
}
