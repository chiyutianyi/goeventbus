package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/chiyutianyi/goeventbus/queue"
)

var e *EventBus
var lock sync.Mutex

// Post Event
func Post(ctx context.Context, event Event) error {
	if e == nil {
		return nil
	}
	eventPost.Inc()
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %s", err)
	}
	log.Debugf("post event %s", msg)
	return e.mq.Publish(ctx, event.GetTopic(), string(msg))
}

func Register(topic string, subscriber *Subscriber) error {
	log.Debugf("Registering topic %s", topic)
	if e == nil {
		log.Debugf("event bus not initialized")
		return nil
	}
	return e.Register(topic, subscriber)
}

type eventMsg struct {
	Topic string `json:"topic"`
	Msg   string `json:"msg"`
}

type EventBus struct {
	mq          queue.MessageQueue
	subscribers map[string]*Subscriber

	ch chan eventMsg

	workers int
}

func Initialize(mq queue.MessageQueue, workers int) error {
	lock.Lock()
	defer lock.Unlock()
	if e != nil {
		return fmt.Errorf("event bus already initialized")
	}
	e = &EventBus{
		mq:          mq,
		subscribers: map[string]*Subscriber{},
		ch:          make(chan eventMsg),
		workers:     workers,
	}
	return nil
}

func Serve() {
	if e == nil {
		return
	}
	workers := e.workers
	if workers <= 0 {
		workers = 5
	}
	go e.Serve(workers)
}

// Register registers the event to the topic
func (e *EventBus) Register(topic string, subscriber *Subscriber) error {
	if _, ok := e.subscribers[topic]; ok {
		return fmt.Errorf("topic %s already registered", topic)
	}
	e.subscribers[topic] = subscriber
	return nil
}

func (e *EventBus) handle(topic, msg string) error {
	var (
		subscriber *Subscriber
		ok         bool
	)

	if subscriber, ok = e.subscribers[topic]; !ok {
		return fmt.Errorf("topic %s not registered", topic)
	}

	event := subscriber.NewEvent()

	if err := json.Unmarshal([]byte(msg), &event); err != nil {
		return fmt.Errorf("unmarshal message: %s", err)
	}

	if topic != event.GetTopic() {
		log.Warnf("dirty topic which should be %s but got %s", topic, event.GetTopic())
		return nil
	}

	eventHandle.Inc()
	log.Debugf("handle event: %v", msg)
	if err := subscriber.Handler(event); err != nil {
		return fmt.Errorf("handle evetn %s : %s", msg, err)
	}
	return nil
}

func (e *EventBus) Serve(workers int) {
	wg := sync.WaitGroup{}
	ctx := context.Background()

	topics := []string{}

	for topic := range e.subscribers {
		topics = append(topics, topic)
	}

	if len(topics) == 0 {
		log.Infof("no topic to subscribe")
		return
	}

	log.Infof("Subscribing to topics %s", topics)
	go e.mq.Subscribe(
		ctx,
		topics,
		func(topic, msg string) error {
			e.ch <- eventMsg{topic, msg}
			return nil
		},
	)

	log.Debugf("Start event bus with %d subscribers", workers)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		num := i
		go func() {
			defer wg.Done()
			for {
				msg := <-e.ch
				log.Debugf("worker %d handling event %s", num, msg)
				if err := e.handle(msg.Topic, msg.Msg); err != nil {
					log.Errorf("handle : %s", err)
				}
			}
		}()
	}
	wg.Wait()
}
