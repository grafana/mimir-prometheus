package tsdb

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/DmitriyVTitov/size"
	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

const (
	// NOTE: keep them exported to reference them in Mimir.

	DefaultPostingsForMatchersCacheTTL      = 10 * time.Second
	DefaultPostingsForMatchersCacheMaxItems = 100
	DefaultPostingsForMatchersCacheMaxBytes = 10 * 1024 * 1024 // Based on the default max items, 10MB / 100 = 100KB per cached entry on average.
	DefaultPostingsForMatchersCacheForce    = false
)

// IndexPostingsReader is a subset of IndexReader methods, the minimum required to evaluate PostingsForMatchers.
type IndexPostingsReader interface {
	// LabelValues returns possible label values which may not be sorted.
	LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error)

	// Postings returns the postings list iterator for the label pairs.
	// The Postings here contain the offsets to the series inside the index.
	// Found IDs are not strictly required to point to a valid Series, e.g.
	// during background garbage collections. Input values must be sorted.
	Postings(ctx context.Context, name string, values ...string) (index.Postings, error)
}

// NewPostingsForMatchersCache creates a new PostingsForMatchersCache.
// If `ttl` is 0, then it only deduplicates in-flight requests.
// If `force` is true, then all requests go through cache, regardless of the `concurrent` param provided to the PostingsForMatchers method.
func NewPostingsForMatchersCache(ttl time.Duration, maxItems int, maxBytes int64, force bool) *PostingsForMatchersCache {
	b := &PostingsForMatchersCache{
		calls:  map[string]*postingsForMatchersPromise{},
		cached: list.New(),

		ttl:      ttl,
		maxItems: maxItems,
		maxBytes: maxBytes,
		force:    force,

		timeNow:             time.Now,
		postingsForMatchers: PostingsForMatchers,

		tracer:      otel.Tracer(""),
		ttlAttrib:   attribute.Stringer("ttl", ttl),
		forceAttrib: attribute.Bool("force", force),
	}

	return b
}

// PostingsForMatchersCache caches PostingsForMatchers call results when the concurrent hint is passed in or force is true.
type PostingsForMatchersCache struct {
	calls map[string]*postingsForMatchersPromise

	cachedMtx   sync.RWMutex
	cached      *list.List
	cachedBytes int64

	ttl      time.Duration
	maxItems int
	maxBytes int64
	force    bool

	// timeNow is the time.Now that can be replaced for testing purposes
	timeNow func() time.Time
	// postingsForMatchers can be replaced for testing purposes
	postingsForMatchers func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error)

	tracer trace.Tracer
	// Preallocated for performance
	ttlAttrib   attribute.KeyValue
	forceAttrib attribute.KeyValue
}

func (c *PostingsForMatchersCache) PostingsForMatchers(ctx context.Context, ix IndexPostingsReader, concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	if !concurrent && !c.force {
		return c.postingsForMatchers(ctx, ix, ms...)
	}
	c.expire()

	key := matchersKey(ms)
	// Protect cache state from concurrent write access
	c.cachedMtx.RLock()

	promise := c.calls[key]
	if promise != nil {
		// Let's wait on promise to be done
		promise.waiting.Inc()
		c.cachedMtx.RUnlock()
		return c.waitOnPromise(ctx, promise, key)
	}

	c.cachedMtx.RUnlock()

	c.cachedMtx.Lock()
	if c.calls[key] != nil {
		// A promise has been injected into the cache in the meantime
		promise = c.calls[key]
		promise.waiting.Inc()
		c.cachedMtx.Unlock()

		return c.waitOnPromise(ctx, promise, key)
	}

	queryCtx, cancel := context.WithCancelCause(context.Background())
	promise = &postingsForMatchersPromise{
		done:    make(chan int64),
		waiting: atomic.NewInt32(1),
		cancel:  cancel,
	}
	c.calls[key] = promise
	c.cachedMtx.Unlock()

	go func() {
		// Close promise channel after fulfilment
		defer close(promise.done)

		// Don't let context cancellation fail the promise, since it may be used by multiple goroutines, each with
		// its own context. Also, keep the call independent of this particular context, since the promise will be reused.
		postings, err := c.postingsForMatchers(queryCtx, ix, ms...)
		if err != nil {
			promise.err = err
			if errors.Is(promise.err, context.DeadlineExceeded) || errors.Is(promise.err, context.Canceled) {
				// Canceled queries will be pruned from the cache
				return
			}
		} else {
			if queryCtx.Err() != nil {
				// Canceled queries will be pruned from the cache
				return
			}

			promise.cloner = index.NewPostingsCloner(postings)
		}

		sizeBytes := int64(len(key) + size.Of(promise))
		promise.done <- sizeBytes
	}()

	return c.waitOnPromise(ctx, promise, key)
}

type postingsForMatchersPromise struct {
	// done signals when the promise is fulfilled, sending the cache entry size for storing by a consumer
	done    chan int64
	waiting *atomic.Int32
	cancel  context.CancelCauseFunc

	cloner *index.PostingsCloner
	err    error
}

// waitOnPromise waits until either the context is done or the promise is.
// If the context is done, the number of waiting is decremented, and the promise gets canceled if the
// number of waiting has reached 0.
func (c *PostingsForMatchersCache) waitOnPromise(ctx context.Context, promise *postingsForMatchersPromise, key string) (index.Postings, error) {
	select {
	case <-ctx.Done():
		// The request was canceled, reduce the waiting count
		c.cachedMtx.Lock()
		waiting := promise.waiting.Dec()
		if waiting > 0 {
			// Promise is in use by other goroutines
			c.cachedMtx.Unlock()
			return nil, errors.Wrap(ctx.Err(), "PostingsForMatchers context error")
		}

		// There are no more waiting goroutines, cancel the promise and remove it from the cache
		promise.cancel(fmt.Errorf("no remaining callers interested in query"))
		delete(c.calls, key)
		c.cachedMtx.Unlock()

		// Wait for query execution goroutine to finish
		<-promise.done
		return nil, errors.Wrap(ctx.Err(), "PostingsForMatchers context error")
	case sizeBytes := <-promise.done:
		// Checking context error is necessary for deterministic tests,
		// as channel selection order is random
		if ctx.Err() != nil {
			return nil, errors.Wrap(ctx.Err(), "PostingsForMatchers context error")
		}

		if sizeBytes > 0 {
			// Got the promise cache entry's size, store it
			c.created(key, c.timeNow(), sizeBytes)
		}

		if promise.err != nil {
			return nil, promise.err
		}

		return promise.cloner.Clone(), nil
	}
}

type postingsForMatchersCachedCall struct {
	key string
	ts  time.Time

	// Size of the cached entry, in bytes.
	sizeBytes int64
}

func (c *PostingsForMatchersCache) expire() {
	if c.ttl <= 0 {
		return
	}

	c.cachedMtx.RLock()
	if !c.shouldEvictHead() {
		c.cachedMtx.RUnlock()
		return
	}
	c.cachedMtx.RUnlock()

	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()

	for c.shouldEvictHead() {
		c.evictHead()
	}
}

// shouldEvictHead returns true if cache head should be evicted, either because it's too old,
// or because the cache has too many elements
// should be called while read lock is held on cachedMtx.
func (c *PostingsForMatchersCache) shouldEvictHead() bool {
	// The cache should be evicted for sure if the max size (either items or bytes) is reached.
	if c.cached.Len() > c.maxItems || c.cachedBytes > c.maxBytes {
		return true
	}

	h := c.cached.Front()
	if h == nil {
		return false
	}
	ts := h.Value.(*postingsForMatchersCachedCall).ts
	return c.timeNow().Sub(ts) >= c.ttl
}

func (c *PostingsForMatchersCache) evictHead() {
	front := c.cached.Front()
	oldest := front.Value.(*postingsForMatchersCachedCall)
	delete(c.calls, oldest.key)
	c.cached.Remove(front)
	c.cachedBytes -= oldest.sizeBytes
}

// created has to be called when returning from the PostingsForMatchers call that creates the promise.
// the ts provided should be the call time.
func (c *PostingsForMatchersCache) created(key string, ts time.Time, sizeBytes int64) {
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()

	if c.ttl <= 0 {
		delete(c.calls, key)
		return
	}

	c.cached.PushBack(&postingsForMatchersCachedCall{
		key:       key,
		ts:        ts,
		sizeBytes: sizeBytes,
	})
	c.cachedBytes += sizeBytes
	span.AddEvent("added cached value to expiry queue", trace.WithAttributes(
		attribute.Stringer("timestamp", ts),
		attribute.Int64("size in bytes", sizeBytes),
		attribute.Int64("cached bytes", c.cachedBytes),
	))
}

// matchersKey provides a unique string key for the given matchers slice.
// NOTE: different orders of matchers will produce different keys,
// but it's unlikely that we'll receive same matchers in different orders at the same time.
func matchersKey(ms []*labels.Matcher) string {
	const (
		typeLen = 2
		sepLen  = 1
	)
	var size int
	for _, m := range ms {
		size += len(m.Name) + len(m.Value) + typeLen + sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
	for _, m := range ms {
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
		sb.WriteByte(0)
	}
	key := sb.String()
	return key
}

// indexReaderWithPostingsForMatchers adapts an index.Reader to be an IndexReader by adding the PostingsForMatchers method.
type indexReaderWithPostingsForMatchers struct {
	*index.Reader
	pfmc *PostingsForMatchersCache
}

func (ir indexReaderWithPostingsForMatchers) PostingsForMatchers(ctx context.Context, concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	return ir.pfmc.PostingsForMatchers(ctx, ir, concurrent, ms...)
}

var _ IndexReader = indexReaderWithPostingsForMatchers{}
