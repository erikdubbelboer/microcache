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
	Cache *ristretto.Cache[string, any]
}

func calculateResponseCost(res Response) int64 {
	s := responseSize

	// Estimate size of the map itself.
	s += 5*8 + int64(len(res.header)*8)

	for k, vv := range res.header {
		s += int64(len(k))
		for _, v := range vv {
			s += int64(len(v))
		}
	}

	s += int64(cap(res.body))
	s += int64(len(res.hash))

	return s
}

func calculateRequestOptCost(req RequestOpts) int64 {
	s := requestOptsSize

	for _, v := range req.vary {
		s += int64(len(v))
	}
	for _, v := range req.varyQuery {
		s += int64(len(v))
	}

	s += int64(len(req.hash))

	return s
}

// NewDriverRistretto returns the default Ristretto driver configuration.
// requests should be the number of items you expect to keep in the cache when full.
// Estimating this on the higher side is better.
// size determines the maximum number of bytes in the cache.
func NewDriverRistretto(requests, size int64) DriverRistretto {
	cache, err := ristretto.NewCache[string, any](&ristretto.Config[string, any]{
		NumCounters: requests * 10,
		MaxCost:     size,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		panic(err)
	}

	return DriverRistretto{cache}
}

func (d DriverRistretto) SetRequestOpts(hash string, req RequestOpts) error {
	req.hash = hash
	d.Cache.Set(hash, req, calculateRequestOptCost(req))
	return nil
}

func (d DriverRistretto) GetRequestOpts(hash string) (req RequestOpts, collision bool) {
	r, ok := d.Cache.Get(hash)
	if ok && r != nil {
		req, ok = r.(RequestOpts)
		if !ok {
			_, ok := r.(Response)
			if ok {
				return req, true
			}
		} else if req.hash != hash {
			return req, true
		}
	}
	return req, false
}

func (d DriverRistretto) Set(hash string, res Response) error {
	res.hash = hash
	d.Cache.Set(hash, res, calculateResponseCost(res))
	return nil
}

func (d DriverRistretto) Get(hash string) (res Response, collision bool) {
	r, ok := d.Cache.Get(hash)
	if ok && r != nil {
		res, ok = r.(Response)
		if !ok {
			_, ok := r.(RequestOpts)
			if ok {
				return res, true
			}
		} else if res.hash != hash {
			return res, true
		}
	}
	return res, false
}

func (d DriverRistretto) Remove(hash string) error {
	d.Cache.Del(hash)
	return nil
}

func (d DriverRistretto) GetSize() int {
	return int(d.Cache.Metrics.KeysAdded() - d.Cache.Metrics.KeysEvicted())
}
