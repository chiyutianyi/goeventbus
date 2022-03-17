package example

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/chiyutianyi/goeventbus/queue"
)

const (
	QueueRedisURL      = "queue_redis_url"
	QueueRedisUsername = "queue_redis_username"
	QueueRedisPassword = "queue_redis_password"
)

type redisMQ struct {
	rdb *redis.Client
}

func NewRedisMq() (queue.MessageQueue, error) {
	url := viper.GetString(QueueRedisURL)
	if url == "" {
		return nil, fmt.Errorf("message queue type is redis, but %s not given", QueueRedisURL)
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %s", url, err)
	}
	if opt.Username == "" {
		opt.Username = viper.GetString(QueueRedisUsername)
	}
	if opt.Password == "" {
		opt.Password = viper.GetString(QueueRedisPassword)
	}
	opt.MaxRetries = 5
	opt.MinRetryBackoff = time.Millisecond * 100
	opt.MaxRetryBackoff = time.Minute * 1
	opt.ReadTimeout = time.Second * 30
	opt.WriteTimeout = time.Second * 5
	rdb := redis.NewClient(opt)
	return &redisMQ{rdb: rdb}, nil
}

// Publish publishes an event to the queue.
func (e *redisMQ) Publish(ctx context.Context, topic, msg string) error {
	return e.rdb.Publish(ctx, topic, msg).Err()
}

func (e *redisMQ) Subscribe(ctx context.Context, topics []string, handle func(topic, msg string) error) {
	subscriber := e.rdb.Subscribe(ctx, topics...)

	for {
		msg, err := subscriber.ReceiveMessage(ctx)
		if err != nil {
			log.Errorf("receive message: %s", err)
			continue
		}

		if err := handle(msg.Channel, msg.Payload); err != nil {
			log.Errorf("handle message: %s", err)
		}
	}
}

func init() {
	queue.Register("redis", NewRedisMq)
}
