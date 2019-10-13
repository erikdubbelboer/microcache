package microcache

import (
	"time"
)

// Monitor is an interface for collecting metrics about the microcache
type Monitor interface {
	GetInterval() time.Duration
	Log(Stats)
	Hit()
	Miss()
	Stale()
	Backend()
	Error()
	Collision()
}

type Stats struct {
	Size       int
	Hits       int
	Misses     int
	Stales     int
	Backend    int
	Errors     int
	Collisions int
}
