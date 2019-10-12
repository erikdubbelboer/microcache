package microcache

import (
	"unsafe"

	"github.com/dgraph-io/ristretto"
)

var (
	requestOptsSize = int64(unsafe.Sizeof(RequestOpts{}))
	responseSize    = int64(unsafe.Sizeof(Response{}))
)

// DriverRistretto is a driver implementation using github.com/dgraph-io/ristretto
type DriverRistretto struct {
	Cache *ristretto.Cache
}

func calculateResponseCost(res Response) int64 {
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
// requests should be the number of items you expect to keep in the cache when full.
// Estimating this on the higher side is better.
// size determines the maximum number of bytes in the cache.
func NewDriverRistretto(requests, size int64) DriverRistretto {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: requests * 10,
		MaxCost:     size,
		BufferItems: 64,
		Metrics:     false,
	})
	if err != nil {
		panic(err)
	}

	return DriverRistretto{cache}
}

func (d DriverRistretto) SetRequestOpts(hash string, req RequestOpts) error {
	d.Cache.Set(hash, req, requestOptsSize)
	return nil
}

func (d DriverRistretto) GetRequestOpts(hash string) (req RequestOpts) {
	r, ok := d.Cache.Get(hash)
	if ok && r != nil {
		req = r.(RequestOpts)
	}
	return req
}

func (d DriverRistretto) Set(hash string, res Response) error {
	d.Cache.Set(hash, res, calculateResponseCost(res))
	return nil
}

func (d DriverRistretto) Get(hash string) (res Response) {
	r, ok := d.Cache.Get(hash)
	if ok && r != nil {
		res = r.(Response)
	}
	return res
}

func (d DriverRistretto) Remove(hash string) error {
	d.Cache.Del(hash)
	return nil
}

func (d DriverRistretto) GetSize() int {
	return -1
}
