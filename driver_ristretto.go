package microcache

import (
	"sync"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/dgraph-io/ristretto/z"
)

var (
	responseSize = int64(unsafe.Sizeof(Response{}))
)

// DriverRistretto is a driver implementation using github.com/dgraph-io/ristretto
type DriverRistretto struct {
	Cache *ristretto.Cache

	opts   map[uint64]RequestOpts
	optsMu sync.Mutex
}

func calculateCost(res Response) int64 {
	s := int64(0)

	for k, vv := range res.header {
		s += int64(len(k))
		for _, v := range vv {
			s += int64(len(v))
		}
	}

	s += int64(len(res.body))
	s += responseSize

	return s
}

// NewDriverRistretto returns the default Ristretto driver configuration.
// size determines the number of bytes in the cache.
func NewDriverRistretto(size int64) *DriverRistretto {
	d := &DriverRistretto{
		opts: make(map[uint64]RequestOpts, 0),
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10000000,
		MaxCost:     size,
		BufferItems: 64,
		Metrics:     false,
		OnEvict: func(key uint64, value interface{}, cost int64) {
			d.optsMu.Lock()
			delete(d.opts, key)
			d.optsMu.Unlock()
		},
	})
	if err != nil {
		panic(err)
	}

	d.Cache = cache

	return d
}

func (d *DriverRistretto) SetRequestOpts(hash string, req RequestOpts) error {
	key := z.MemHashString(hash)
	d.optsMu.Lock()
	d.opts[key] = req
	d.optsMu.Unlock()
	return nil
}

func (d *DriverRistretto) GetRequestOpts(hash string) (req RequestOpts) {
	key := z.MemHashString(hash)
	d.optsMu.Lock()
	req = d.opts[key]
	d.optsMu.Unlock()
	return
}

func (d *DriverRistretto) Set(hash string, res Response) error {
	d.Cache.Set(hash, res, calculateCost(res))
	return nil
}

func (d *DriverRistretto) Get(hash string) (res Response) {
	r, ok := d.Cache.Get(hash)
	if ok && r != nil {
		res = r.(Response)
	}
	return res
}

func (d *DriverRistretto) Remove(hash string) error {
	d.Cache.Del(hash)
	return nil
}

func (d *DriverRistretto) GetSize() int {
	d.optsMu.Lock()
	l := len(d.opts)
	d.optsMu.Unlock()
	return l
}
