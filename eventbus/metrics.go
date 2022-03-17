package eventbus

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	eventPost = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "eventbus_post",
		Help: "post events",
	})
	eventHandle = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "eventbus_handle",
		Help: "handle events",
	})
)

func init() {
	_ = prometheus.Register(eventPost)
	_ = prometheus.Register(eventHandle)
}
