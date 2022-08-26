package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	logs "github.com/sirupsen/logrus"
)

var (
	subscriberTimeout = 10 * time.Second
)

type eventMsg struct {
	Topic string `json:"topic"`
	Msg   string `json:"msg"`
}

// messageQueue is the interface for message queue
type messageQueue interface {
	// Publish publishes the event to all Subscribers
	Publish(ctx context.Context, topic, msg string) error
	// Subscribe subscribes to the topic
	Subscribe(ctx context.Context, topics []string, handler func(topic, msg string) error)
}

// Eventbus is the interface for event bus
type Eventbus interface {
	// Register registers the event to the topic
	Register(topic string, subscribers ...*Subscriber) error
	// Post posts the event to the topic
	Post(ctx context.Context, event Event) error
	// Serve starts the event bus async
	Serve(ctx context.Context)
}

type eventbus struct {
	sync.RWMutex
	mq          messageQueue
	subscribers map[string]*Subscribers

	ch chan eventMsg

	workers int
}

// NewEventbus creates the event bus
func NewEventbus(mq messageQueue, workers int) Eventbus {
	if workers <= 0 {
		workers = 5
	}
	return &eventbus{
		mq:          mq,
		subscribers: map[string]*Subscribers{},
		ch:          make(chan eventMsg, 1000),
		workers:     workers,
	}
}

// Post posts the event to the topic
func (e *eventbus) Post(ctx context.Context, event Event) error {
	eventsQueued.Inc()
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %s", err)
	}
	logs.Debugf("post event %s", msg)
	return e.mq.Publish(ctx, event.GetTopic(), string(msg))
}

// Register registers the event to the topic
func (e *eventbus) Register(topic string, ins ...*Subscriber) error {
	var (
		subs *Subscribers
		ok   bool
	)
	logs.Debugf("Registering topic %s", topic)
	e.Lock()
	defer e.Unlock()
	if subs, ok = e.subscribers[topic]; !ok {
		subs = newSubscribers()
		e.subscribers[topic] = subs
	}
	for _, subscriber := range ins {
		if subs.Contains(subscriber.UID()) {
			panic(fmt.Sprintf("subscriber %s already registered", subscriber.UID()))
		}
		subs.Add(subscriber)
	}
	return nil
}

func (e *eventbus) handle(topic, msg string) error {
	var (
		subs    *Subscribers
		subsLen int
		ok      bool
		wg      sync.WaitGroup
	)

	e.RLock()
	if subs, ok = e.subscribers[topic]; !ok {
		e.RUnlock()
		return fmt.Errorf("topic %s not registered", topic)
	}
	e.RUnlock()

	if subsLen = subs.Len(); subsLen == 0 {
		logs.Warnf("no subscriber for topic %s", topic)
		return nil
	}

	eventsQueued.Dec()

	errs := make(chan error, subsLen)

	subs.Each(func(sub *Subscriber) {
		wg.Add(1)
		go func(subscriber *Subscriber) {
			defer func() {
				if r := recover(); r != nil {
					logs.Errorf("subscriber %s panic: %v", subscriber.UID(), r)
					errs <- fmt.Errorf("subscriber %s panic: %v", subscriber.UID(), r)
				}
				wg.Done()
			}()
			event := subscriber.NewEvent()

			if err := json.Unmarshal([]byte(msg), &event); err != nil {
				logs.Errorf("unmarshal message: %s", err)
				errs <- err
				return
			}

			if topic != event.GetTopic() {
				logs.Warnf("dirty topic which should be %s but got %s", topic, event.GetTopic())
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), subscriberTimeout)
			defer cancel()

			logs.Debugf("handle event: %v", msg)
			if err := subscriber.HandleEvent(ctx, event); err != nil {
				logs.Errorf("handle event %s : %s", msg, err)
				errs <- err
			}
		}(sub)
	})

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("handle event: %s", <-errs)
	}

	return nil
}

// Serve starts the event bus async
func (e *eventbus) Serve(ctx context.Context) {
	var (
		wg     sync.WaitGroup
		topics []string
	)

	e.RLock()
	for topic := range e.subscribers {
		topics = append(topics, topic)
	}
	e.RUnlock()

	if len(topics) == 0 {
		logs.Infof("no topic to subscribe")
		return
	}

	logs.Infof("Subscribing to topics %s", topics)
	e.mq.Subscribe(
		ctx,
		topics,
		func(topic, msg string) error {
			e.ch <- eventMsg{topic, msg}
			return nil
		},
	)
	logs.Debugf("Start event bus with %d Subscribers", e.workers)
	for i := 0; i < e.workers; i++ {
		wg.Add(1)
		go func(num int) {
			wg.Done()
			for {
				select {
				case msg := <-e.ch:
					func() {
						defer func() {
							if r := recover(); r != nil {
								logs.Errorf("panic in event handler: %s", r)
							}
						}()
						logs.Debugf("worker %d handling event %s", num, msg)
						if err := e.handle(msg.Topic, msg.Msg); err != nil {
							logs.Errorf("handle : %s", err)
						}
					}()
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond): // sleep a while
					continue
				}
			}
		}(i)
	}
	wg.Wait()
}
