package eventbus

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	eventsQueued = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "eventbus_queued",
		Help: "events queued",
	})
)

func init() {
	_ = prometheus.Register(eventsQueued)
}
