// microcache is a non-standard HTTP microcache implemented as Go middleware.
package microcache

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Microcache interface {
	Middleware(http.Handler) http.Handler
	Start()
	Stop()
	offsetIncr(time.Duration)
}

type microcache struct {
	Nocache              bool
	Timeout              time.Duration
	TTL                  time.Duration
	StaleIfError         time.Duration
	StaleRecache         bool
	StaleWhileRevalidate time.Duration
	HashQuery            bool
	QueryIgnore          map[string]bool
	CollapsedForwarding  bool
	Vary                 []string
	Driver               Driver
	Compressor           Compressor
	Monitor              Monitor
	Exposed              bool
	SuppressAgeHeader    bool

	stopMonitor     chan bool
	revalidating    map[string]bool
	revalidateMutex *sync.Mutex
	collapse        map[string]*sync.Mutex
	collapseMutex   *sync.Mutex

	// Used to advance time for testing
	offset      time.Duration
	offsetMutex *sync.RWMutex
}

type Config struct {
	// Nocache prevents responses from being cached by default
	// Can be overridden by the microcache-cache and microcache-nocache response headers
	Nocache bool

	// Timeout specifies the maximum execution time for backend responses
	// Example: If the underlying handler takes more than 10s to respond,
	// the request is cancelled and the response is treated as 503
	// Recommended: 10s
	// Default: 0
	Timeout time.Duration

	// TTL specifies a default ttl for cached responses
	// Can be overridden by the microcache-ttl response header
	// Recommended: 10s
	// Default: 0
	TTL time.Duration

	// StaleWhileRevalidate specifies a period during which a stale response may be
	// served immediately while the resource is fetched in the background. This can be
	// useful for ensuring consistent response times at the cost of content freshness.
	// More Info: https://tools.ietf.org/html/rfc5861
	// Recommended: 20s
	// Default: 0
	StaleWhileRevalidate time.Duration

	// StaleIfError specifies a default stale grace period
	// If a request fails and StaleIfError is set, the object will be served as stale
	// and the response will be re-cached for the duration of this grace period
	// Can be overridden by the microcache-ttl-stale response header
	// More Info: https://tools.ietf.org/html/rfc5861
	// Recommended: 20s
	// Default: 0
	StaleIfError time.Duration

	// StaleRecache specifies whether to re-cache the response object for ttl while serving
	// stale response on backend error
	// Recommended: true
	// Default: false
	StaleRecache bool

	// CollapsedForwarding specifies whether to collapse duplicate requests
	// This helps prevent servers with a cold cache from hammering the backend
	// Default: false
	CollapsedForwarding bool

	// HashQuery determines whether all query parameters in the request URI
	// should be hashed to differentiate requests
	// Default: false
	HashQuery bool

	// QueryIgnore is a list of query parameters to ignore when hashing
	// Default: nil
	QueryIgnore []string

	// Vary specifies a list of http request headers by which all requests
	// should be differentiated. When making use of this option, it may be a good idea
	// to normalize these headers first using a separate piece of middleware.
	//
	//   []string{"accept-language", "accept-encoding", "xml-http-request"}
	//
	// Default: []string{}
	Vary []string

	// Driver specifies a cache storage driver
	// Default: lru with 10,000 item capacity
	Driver Driver

	// Compressor specifies a compressor to use for reducing the memory required to cache
	// response bodies
	// Default: nil
	Compressor Compressor

	// Monitor is an optional parameter which will periodically report statistics about
	// the cache to enable monitoring of cache size, cache efficiency and error rate
	// Default: nil
	Monitor Monitor

	// Exposed determines whether to add a header to the response indicating the response state
	// Microcache: ( HIT | MISS | STALE )
	// Default: false
	Exposed bool

	// SuppressAgeHeader determines whether to suppress the age header in responses
	// The age header is added by default to all HIT and STALE responses
	// Age: ( seconds )
	// Default: false
	SuppressAgeHeader bool
}

// New creates and returns a configured microcache instance
func New(o Config) *microcache {
	// Defaults
	m := microcache{
		Nocache:              o.Nocache,
		TTL:                  o.TTL,
		StaleIfError:         o.StaleIfError,
		StaleRecache:         o.StaleRecache,
		StaleWhileRevalidate: o.StaleWhileRevalidate,
		Timeout:              o.Timeout,
		HashQuery:            o.HashQuery,
		CollapsedForwarding:  o.CollapsedForwarding,
		Vary:                 o.Vary,
		Driver:               o.Driver,
		Compressor:           o.Compressor,
		Monitor:              o.Monitor,
		Exposed:              o.Exposed,
		SuppressAgeHeader:    o.SuppressAgeHeader,
		revalidating:         map[string]bool{},
		revalidateMutex:      &sync.Mutex{},
		collapse:             map[string]*sync.Mutex{},
		collapseMutex:        &sync.Mutex{},
		offsetMutex:          &sync.RWMutex{},
	}
	if o.Driver == nil {
		m.Driver = NewDriverLRU(1e4) // default 10k cache items
	}
	if o.QueryIgnore != nil {
		m.QueryIgnore = make(map[string]bool)
		for _, key := range o.QueryIgnore {
			m.QueryIgnore[key] = true
		}
	}
	m.Start()
	return &m
}

// Middleware can be used to wrap an HTTP handler with microcache functionality.
// It can also be passed to http middleware providers like alice as a constructor.
//
//     mx := microcache.New(microcache.Config{TTL: 10 * time.Second})
//     newHandler := mx.Middleware(yourHandler)
//
// Or with alice
//
//    chain.Append(mx.Middleware)
//
func (m *microcache) Middleware(h http.Handler) http.Handler {
	if m.Timeout > 0 {
		h = http.TimeoutHandler(h, m.Timeout, "Timed out")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Websocket passthrough
		upgrade := strings.ToLower(r.Header.Get("connection")) == "upgrade"
		if upgrade || m.Driver == nil {
			if m.Monitor != nil {
				m.Monitor.Miss()
			}
			h.ServeHTTP(w, r)
			return
		}

		// Fetch request options
		reqHash := getRequestHash(m, r)
		req, collision := m.Driver.GetRequestOpts(reqHash)

		if collision {
			if m.Monitor != nil {
				m.Monitor.Collision()
			}
		}

		// Hard passthrough on non cacheable requests
		if req.nocache {
			if m.Monitor != nil {
				m.Monitor.Miss()
			}
			h.ServeHTTP(w, r)
			return
		}

		// CollapsedForwarding
		// This implementation may collapse too many uncacheable requests.
		// Refactor may be complicated.
		if m.CollapsedForwarding {
			m.collapseMutex.Lock()
			mutex, ok := m.collapse[reqHash]
			if !ok {
				mutex = &sync.Mutex{}
				m.collapse[reqHash] = mutex
			}
			m.collapseMutex.Unlock()
			// Mutex serializes collapsible requests
			mutex.Lock()
			defer func() {
				mutex.Unlock()
				m.collapseMutex.Lock()
				delete(m.collapse, reqHash)
				m.collapseMutex.Unlock()
			}()
			if !req.found {
				req, collision = m.Driver.GetRequestOpts(reqHash)
				if collision {
					if m.Monitor != nil {
						m.Monitor.Collision()
					}
				}
			}
		}

		// Fetch cached response object
		var objHash string
		var obj Response
		if req.found {
			objHash = req.getObjectHash(reqHash, r)
			obj, collision = m.Driver.Get(objHash)
			if collision {
				if m.Monitor != nil {
					m.Monitor.Collision()
				}
			}
			if m.Compressor != nil {
				obj = m.Compressor.Expand(obj)
			}
		}

		// Non-cacheable request method passthrough and purge
		if r.Method != "GET" && r.Method != "HEAD" && r.Method != "OPTIONS" {
			if m.Monitor != nil {
				m.Monitor.Miss()
			}
			if obj.found {
				// HTTP spec requires caches to purge cached responses following
				// successful unsafe request
				ptw := passthroughWriter{w, 0}
				h.ServeHTTP(&ptw, r)
				if ptw.status >= 200 && ptw.status < 400 {
					m.Driver.Remove(objHash)
				}
			} else {
				h.ServeHTTP(w, r)
			}
			return
		}

		// Fresh response object found
		if obj.found && obj.expires.After(m.now()) {
			if m.Monitor != nil {
				m.Monitor.Hit()
			}
			if m.Exposed {
				w.Header().Set("microcache", "HIT")
			}
			m.setAgeHeader(w, obj)
			obj.sendResponse(w)
			return
		}

		// Stale While Revalidate
		if obj.found && req.staleWhileRevalidate > 0 &&
			obj.expires.Add(req.staleWhileRevalidate).After(m.now()) {
			if m.Monitor != nil {
				m.Monitor.Stale()
			}
			if m.Exposed {
				w.Header().Set("microcache", "STALE")
			}
			m.setAgeHeader(w, obj)
			obj.sendResponse(w)

			// Dedupe revalidation
			m.revalidateMutex.Lock()
			_, revalidating := m.revalidating[objHash]
			if !revalidating {
				m.revalidating[objHash] = true
			}
			m.revalidateMutex.Unlock()
			if !revalidating {
				br := newBackgroundRequest(r)
				go func() {
					defer func() {
						// Clear revalidation lock
						m.revalidateMutex.Lock()
						delete(m.revalidating, objHash)
						m.revalidateMutex.Unlock()
					}()
					m.handleBackendResponse(h, w, br, reqHash, req, objHash, obj, true)
				}()
			}

			return
		} else {
			m.handleBackendResponse(h, w, r, reqHash, req, objHash, obj, false)
			return
		}
	})
}

func (m *microcache) handleBackendResponse(
	h http.Handler,
	w http.ResponseWriter,
	r *http.Request,
	reqHash string,
	req RequestOpts,
	objHash string,
	obj Response,
	background bool,
) {
	if m.Monitor != nil {
		m.Monitor.Backend()
	}

	// Backend Response
	beres := Response{header: http.Header{}}

	// Execute request
	h.ServeHTTP(&beres, r)

	if !beres.headerWritten {
		beres.status = http.StatusOK
	}

	// Log Error
	if beres.status >= 500 && m.Monitor != nil {
		m.Monitor.Error()
	}

	// Serve Stale
	if beres.status >= 500 && obj.found {
		serveStale := obj.expires.Add(req.staleIfError).After(m.now())
		// Extend stale response expiration by staleIfError grace period
		if req.found && serveStale && req.staleRecache {
			obj.expires = obj.date.Add(m.getOffset()).Add(req.ttl)
			m.store(objHash, obj)
		}
		if !background && serveStale {
			if m.Monitor != nil {
				m.Monitor.Stale()
			}
			if m.Exposed {
				w.Header().Set("microcache", "STALE")
			}
			m.setAgeHeader(w, obj)
			obj.sendResponse(w)
			return
		}
	}

	// Backend Request succeeded
	if beres.status >= 200 && beres.status < 400 {
		if !req.found {
			// Store request options
			req = buildRequestOpts(m, beres, r)
			m.Driver.SetRequestOpts(reqHash, req)
			objHash = req.getObjectHash(reqHash, r)
		}
		// Cache response
		if !req.nocache {
			beres.expires = m.now().Add(req.ttl)
			m.store(objHash, beres)
		}
	}

	// Don't render response during background revalidate
	if background {
		return
	}

	if m.Monitor != nil {
		m.Monitor.Miss()
	}
	if m.Exposed {
		w.Header().Set("microcache", "MISS")
	}
	beres.sendResponse(w)
}

// Start starts the monitor and any other required background processes
func (m *microcache) Start() {
	if m.stopMonitor != nil || m.Monitor == nil {
		return
	}
	m.stopMonitor = make(chan bool)
	go func() {
		for {
			select {
			case <-time.After(m.Monitor.GetInterval()):
				m.Monitor.Log(Stats{
					Size: m.Driver.GetSize(),
				})
			case <-m.stopMonitor:
				return
			}
		}
	}()
}

// setAgeHeader sets the age header if not suppressed
func (m *microcache) setAgeHeader(w http.ResponseWriter, obj Response) {
	if !m.SuppressAgeHeader {
		age := (m.now().Unix() - obj.date.Unix())
		w.Header().Set("age", fmt.Sprintf("%d", age))
	}
}

// store sets the age header if not suppressed
func (m *microcache) store(objHash string, obj Response) {
	obj.found = true
	obj.date = time.Now()
	if m.Compressor != nil {
		m.Driver.Set(objHash, m.Compressor.Compress(obj))
	} else {
		m.Driver.Set(objHash, obj)
	}
}

// Stop stops the monitor and any other required background processes
func (m *microcache) Stop() {
	if m.stopMonitor == nil {
		return
	}
	m.stopMonitor <- true
}

// Increments the offset for testing purposes
func (m *microcache) offsetIncr(o time.Duration) {
	m.offsetMutex.Lock()
	defer m.offsetMutex.Unlock()
	m.offset += o
}

// Get offset
func (m *microcache) getOffset() time.Duration {
	m.offsetMutex.RLock()
	defer m.offsetMutex.RUnlock()
	return m.offset
}

// Get current time with offset
func (m *microcache) now() time.Time {
	return time.Now().Add(m.getOffset())
}
