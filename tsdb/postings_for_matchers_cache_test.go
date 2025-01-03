package tsdb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DmitriyVTitov/size"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

func TestPostingsForMatchersCache(t *testing.T) {
	// newPostingsForMatchersCache tests the NewPostingsForMatcherCache constructor, but overrides the postingsForMatchers func
	newPostingsForMatchersCache := func(ttl time.Duration, maxItems int, maxBytes int64, pfm func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error), timeMock *timeNowMock, force bool) *PostingsForMatchersCache {
		c := NewPostingsForMatchersCache(ttl, maxItems, maxBytes, force)
		if c.postingsForMatchers == nil {
			t.Fatalf("NewPostingsForMatchersCache() didn't assign postingsForMatchers func")
		}
		c.postingsForMatchers = pfm
		c.timeNow = timeMock.timeNow
		return c
	}

	ctx := context.Background()

	t.Run("happy case one call", func(t *testing.T) {
		for _, concurrent := range []bool{true, false} {
			t.Run(fmt.Sprintf("concurrent=%t", concurrent), func(t *testing.T) {
				expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
				expectedPostingsErr := fmt.Errorf("failed successfully")

				c := newPostingsForMatchersCache(DefaultPostingsForMatchersCacheTTL, 5, 1000, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
					require.IsType(t, indexForPostingsMock{}, ix, "Incorrect IndexPostingsReader was provided to PostingsForMatchers, expected the mock, was given %v (%T)", ix, ix)
					require.Equal(t, expectedMatchers, ms, "Wrong label matchers provided, expected %v, got %v", expectedMatchers, ms)
					return index.ErrPostings(expectedPostingsErr), nil
				}, &timeNowMock{}, false)

				p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, concurrent, expectedMatchers...)
				require.NoError(t, err)
				require.NotNil(t, p)
				require.Equal(t, expectedPostingsErr, p.Err(), "Expected ErrPostings with err %q, got %T with err %q", expectedPostingsErr, p, p.Err())
			})
		}
	})

	t.Run("err returned", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		expectedErr := fmt.Errorf("failed successfully")

		c := newPostingsForMatchersCache(DefaultPostingsForMatchersCacheTTL, 5, 1000, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			return nil, expectedErr
		}, &timeNowMock{}, false)

		_, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, expectedMatchers...)
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("happy case multiple concurrent calls: two same one different", func(t *testing.T) {
		for _, cacheEnabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("cacheEnabled=%t", cacheEnabled), func(t *testing.T) {
				for _, forced := range []bool{true, false} {
					concurrent := !forced
					t.Run(fmt.Sprintf("forced=%t", forced), func(t *testing.T) {
						calls := [][]*labels.Matcher{
							{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},                                                         // 1
							{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},                                                         // 1 same
							{labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar")},                                                        // 2: different match type
							{labels.MustNewMatcher(labels.MatchEqual, "diff", "bar")},                                                        // 3: different name
							{labels.MustNewMatcher(labels.MatchEqual, "foo", "diff")},                                                        // 4: different value
							{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchEqual, "boo", "bam")}, // 5
							{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchEqual, "boo", "bam")}, // 5 same
						}

						// we'll identify results by each call's error, and the error will be the string value of the first matcher
						matchersString := func(ms []*labels.Matcher) string {
							s := strings.Builder{}
							for i, m := range ms {
								if i > 0 {
									s.WriteByte(',')
								}
								s.WriteString(m.String())
							}
							return s.String()
						}
						expectedResults := make([]string, len(calls))
						for i, c := range calls {
							expectedResults[i] = c[0].String()
						}

						const expectedPostingsForMatchersCalls = 5
						// we'll block all the calls until we receive the exact amount. if we receive more, WaitGroup will panic
						called := make(chan struct{}, expectedPostingsForMatchersCalls)
						release := make(chan struct{})
						var ttl time.Duration
						if cacheEnabled {
							ttl = DefaultPostingsForMatchersCacheTTL
						}
						c := newPostingsForMatchersCache(ttl, 5, 1000, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
							select {
							case called <- struct{}{}:
							default:
							}
							<-release
							return nil, fmt.Errorf("%s", matchersString(ms))
						}, &timeNowMock{}, forced)

						results := make([]error, len(calls))
						resultsWg := sync.WaitGroup{}
						resultsWg.Add(len(calls))

						// perform all calls
						for i := 0; i < len(calls); i++ {
							go func(i int) {
								_, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, concurrent, calls[i]...)
								results[i] = err
								resultsWg.Done()
							}(i)
						}

						// wait until all calls arrive to the mocked function
						for i := 0; i < expectedPostingsForMatchersCalls; i++ {
							<-called
						}

						// let them all return
						close(release)

						// wait for the results
						resultsWg.Wait()

						// check that we got correct results
						for i, c := range calls {
							require.ErrorContainsf(t, results[i], matchersString(c), "Call %d should have returned error %q, but got %q instead", i, matchersString(c), results[i])
						}
					})
				}
			})
		}
	})

	t.Run("with concurrent==false, result is not cached", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}

		var call int
		c := newPostingsForMatchersCache(DefaultPostingsForMatchersCacheTTL, 5, 1000, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, &timeNowMock{}, false)

		// first call, fills the cache
		p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, false, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// second call within the ttl (we didn't advance the time), should call again because concurrent==false
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, false, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")
	})

	t.Run("with cache disabled, result is not cached", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}

		var call int
		c := newPostingsForMatchersCache(0, 1000, 1000, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, &timeNowMock{}, false)

		// first call, fills the cache
		p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// second call within the ttl (we didn't advance the time), should call again because concurrent==false
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")
	})

	t.Run("cached value is returned, then it expires", func(t *testing.T) {
		timeNow := &timeNowMock{}
		expectedMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
		}

		var call int
		c := newPostingsForMatchersCache(DefaultPostingsForMatchersCacheTTL, 5, 1000, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, timeNow, false)

		// first call, fills the cache
		p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		timeNow.advance(DefaultPostingsForMatchersCacheTTL / 2)

		// second call within the ttl, should use the cache
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		timeNow.advance(DefaultPostingsForMatchersCacheTTL / 2)

		// third call is after ttl (exactly), should call again
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")
	})

	t.Run("cached value is evicted because cache exceeds max items", func(t *testing.T) {
		const maxItems = 5

		timeNow := &timeNowMock{}
		calls := make([][]*labels.Matcher, maxItems)
		for i := range calls {
			calls[i] = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "matchers", strconv.Itoa(i))}
		}

		callsPerMatchers := map[string]int{}
		c := newPostingsForMatchersCache(DefaultPostingsForMatchersCacheTTL, maxItems, 1000, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			k := matchersKey(ms)
			callsPerMatchers[k]++
			return index.ErrPostings(fmt.Errorf("result from call %d", callsPerMatchers[k])), nil
		}, timeNow, false)

		// each one of the first testCacheSize calls is cached properly
		for _, matchers := range calls {
			// first call
			p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, matchers...)
			require.NoError(t, err)
			require.EqualError(t, p.Err(), "result from call 1")

			// cached value
			p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, matchers...)
			require.NoError(t, err)
			require.EqualError(t, p.Err(), "result from call 1")
		}

		// one extra call is made, which is cached properly, but evicts the first cached value
		someExtraMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		// first call
		p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, someExtraMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// cached value
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, someExtraMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// make first call again, it's calculated again
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, calls[0]...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")
	})

	t.Run("cached value is evicted because cache exceeds max bytes", func(t *testing.T) {
		const (
			maxItems         = 100 // Never hit it.
			maxBytes         = 1250
			numMatchers      = 5
			postingsListSize = 30 // 8 bytes per posting ref, so 30 x 8 = 240 bytes.
		)

		// Generate some matchers.
		matchersLists := make([][]*labels.Matcher, numMatchers)
		for i := range matchersLists {
			matchersLists[i] = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "matchers", strconv.Itoa(i))}
		}

		// Generate some postings lists.
		refsLists := make(map[string][]storage.SeriesRef, numMatchers)
		for i := 0; i < numMatchers; i++ {
			refs := make([]storage.SeriesRef, postingsListSize)
			for r := range refs {
				refs[r] = storage.SeriesRef(i * r)
			}

			refsLists[matchersKey(matchersLists[i])] = refs
		}

		callsPerMatchers := map[string]int{}
		c := newPostingsForMatchersCache(DefaultPostingsForMatchersCacheTTL, maxItems, maxBytes, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			k := matchersKey(ms)
			callsPerMatchers[k]++
			return index.NewListPostings(refsLists[k]), nil
		}, &timeNowMock{}, false)

		// We expect to cache 3 items. So we're going to call PostingsForMatchers for 3 matchers
		// and then double check they're all cached. To do it, we iterate twice.
		for run := 0; run < 2; run++ {
			for i := 0; i < 3; i++ {
				matchers := matchersLists[i]

				p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, matchers...)
				require.NoError(t, err)

				actual, err := index.ExpandPostings(p)
				require.NoError(t, err)
				require.Equal(t, refsLists[matchersKey(matchers)], actual)
			}
		}

		// At this point we expect that the postings have been computed only once for the 3 matchers.
		for i := 0; i < 3; i++ {
			require.Equalf(t, 1, callsPerMatchers[matchersKey(matchersLists[i])], "matcher %d", i)
		}

		// Call PostingsForMatchers() for a 4th matcher. We expect this will evict the oldest cached entry.
		for run := 0; run < 2; run++ {
			matchers := matchersLists[3]

			p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, matchers...)
			require.NoError(t, err)

			actual, err := index.ExpandPostings(p)
			require.NoError(t, err)
			require.Equal(t, refsLists[matchersKey(matchers)], actual)
		}

		// To ensure the 1st (oldest) entry was removed, we call PostingsForMatchers() again on those matchers.
		_, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, matchersLists[0]...)
		require.NoError(t, err)

		require.Equal(t, 2, callsPerMatchers[matchersKey(matchersLists[0])])
		require.Equal(t, 1, callsPerMatchers[matchersKey(matchersLists[1])])
		require.Equal(t, 1, callsPerMatchers[matchersKey(matchersLists[2])])
		require.Equal(t, 1, callsPerMatchers[matchersKey(matchersLists[3])])
	})

	t.Run("initial request context is canceled, no other in-flight requests", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		expectedPostings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))

		var reqCtx context.Context
		var cancelReqCtx context.CancelFunc
		callsCount := atomic.NewInt32(0)

		c := newPostingsForMatchersCache(time.Hour, 5, 1000, func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			callsCount.Inc()

			// We want the request context to be canceled while running PostingsForMatchers()
			// so we call it here. The request context is not the same we're getting in input
			// in this function!
			if cancelReqCtx != nil {
				cancelReqCtx()
			}

			// Wait a short time to see if the context will be canceled. If not, we proceed
			// returning the postings.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()

			case <-time.After(time.Second):
				return expectedPostings.Clone(), nil
			}
		}, &timeNowMock{}, false)

		// Run PostingsForMatchers() a first time, cancelling the context while it's executing.
		reqCtx, cancelReqCtx = context.WithCancel(context.Background())
		_, err := c.PostingsForMatchers(reqCtx, indexForPostingsMock{}, true, matchers...)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int32(1), callsCount.Load())

		// We expect the promise has not been cached.
		require.Equal(t, 0, c.cached.Len())

		// Run PostingsForMatchers() a second time, not cancelling the context. Since the previous
		// canceled execution was not cached, we expect to run it again and get the actual result.
		reqCtx, cancelReqCtx = context.Background(), nil
		actualPostings, err := c.PostingsForMatchers(reqCtx, indexForPostingsMock{}, true, matchers...)
		require.NoError(t, err)
		require.Equal(t, expectedPostings.Clone(), actualPostings)
		require.Equal(t, int32(2), callsCount.Load())
	})

	t.Run("initial request context is cancelled, second request is not cancelled", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		expectedPostings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))

		reqCtx1, cancelReqCtx1 := context.WithCancel(context.Background())
		waitBeforeCancelReqCtx1 := make(chan struct{})
		callsCount := atomic.NewInt32(0)

		c := newPostingsForMatchersCache(time.Hour, 5, 1000, func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			callsCount.Inc()

			// Cancel the initial request once the test sends the signal. The requests context is not the same
			// we're getting in input in this function!
			select {
			case <-waitBeforeCancelReqCtx1:
				cancelReqCtx1()

			case <-time.After(time.Second):
				return nil, errors.New("expected to receive signal to cancel requests but wasn't received")
			}

			// Wait a short time to see if the context will be canceled. If not, we proceed
			// returning the postings.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()

			case <-time.After(time.Second):
				return expectedPostings.Clone(), nil
			}
		}, &timeNowMock{}, false)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()

			_, err := c.PostingsForMatchers(reqCtx1, indexForPostingsMock{}, true, matchers...)
			require.ErrorIs(t, err, context.Canceled)
		}()

		go func() {
			defer wg.Done()

			actualPostings, err := c.PostingsForMatchers(context.Background(), indexForPostingsMock{}, true, matchers...)
			require.NoError(t, err)
			require.Equal(t, expectedPostings.Clone(), actualPostings)
		}()

		// Wait until the 2nd request attaches to the 1st one.
		requirePostingsForMatchesCachePromiseTrackedContexts(t, c, matchersKey(matchers), 2)

		close(waitBeforeCancelReqCtx1)
		wg.Wait()

		// We expect the PostingsForMatchers has been called only once.
		require.Equal(t, int32(1), callsCount.Load())

		// We expect the promise has been cached.
		require.Equal(t, 1, c.cached.Len())

		// Send another request. Since the result was cached, we expect the result is getting picked up
		// from the cache.
		actualPostings, err := c.PostingsForMatchers(context.Background(), indexForPostingsMock{}, true, matchers...)
		require.NoError(t, err)
		require.Equal(t, expectedPostings.Clone(), actualPostings)
		require.Equal(t, int32(1), callsCount.Load())
	})

	t.Run("initial and subsequent requests are canceled during execution", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		postings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))

		reqCtx1, cancelReqCtx1 := context.WithCancel(context.Background())
		reqCtx2, cancelReqCtx2 := context.WithCancel(context.Background())
		cancelRequests := make(chan struct{})
		callsCount := atomic.NewInt32(0)

		c := newPostingsForMatchersCache(time.Hour, 5, 1000, func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			callsCount.Inc()

			// Cancel the requests once the test sends the signal. The requests context is not the same
			// we're getting in input in this function!
			select {
			case <-cancelRequests:
				cancelReqCtx1()
				cancelReqCtx2()

			case <-time.After(time.Second):
				return nil, errors.New("expected to receive signal to cancel requests but wasn't received")
			}

			// Wait a short time to see if the context will be canceled. If not, we proceed
			// returning the postings.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()

			case <-time.After(time.Second):
				return postings.Clone(), nil
			}
		}, &timeNowMock{}, false)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()

			_, err := c.PostingsForMatchers(reqCtx1, indexForPostingsMock{}, true, matchers...)
			require.ErrorIs(t, err, context.Canceled)
		}()

		go func() {
			defer wg.Done()

			_, err := c.PostingsForMatchers(reqCtx2, indexForPostingsMock{}, true, matchers...)
			require.ErrorIs(t, err, context.Canceled)
		}()

		// Wait until the 2nd request attaches to the 1st one.
		requirePostingsForMatchesCachePromiseTrackedContexts(t, c, matchersKey(matchers), 2)

		close(cancelRequests)
		wg.Wait()

		// We expect the promise has not been cached.
		require.Equal(t, 0, c.cached.Len())

		require.Equal(t, int32(1), callsCount.Load())
	})
}

func TestPostingsForMatchersCache_RaceConditionBetweenExecutionContextCancellationAndNewRequest(t *testing.T) {
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	expectedPostings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))

	reqCtx1, cancelReqCtx1 := context.WithCancel(context.Background())
	callsCount := atomic.NewInt32(0)

	wg := sync.WaitGroup{}
	wg.Add(1)

	c := NewPostingsForMatchersCache(time.Hour, 5, 1000, true)

	c.postingsForMatchers = func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
		callsCount.Inc()

		// We want the request context to be canceled while running PostingsForMatchers()
		// so we call it here. The request context is not the same we're getting in input
		// in this function!
		cancelReqCtx1()

		// Wait a short time to see if the context will be canceled. If not, we proceed
		// returning the postings.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-time.After(time.Second):
			return expectedPostings.Clone(), nil
		}
	}

	c.onPromiseExecutionDoneBeforeHook = func() {
		// When the 1st request has been canceled, but before the promise is removed from the calls map,
		// we do issue a 2nd request. The 2nd request is expected to succeed, because its context has
		// not been canceled.
		go func() {
			defer wg.Done()

			actualPostings, err := c.PostingsForMatchers(context.Background(), indexForPostingsMock{}, true, matchers...)
			require.NoError(t, err)
			require.Equal(t, expectedPostings.Clone(), actualPostings)
		}()

		// Wait a (more than) reasonable amount of time to let the 2nd request to kick off.
		time.Sleep(time.Second)
	}

	// Issue the 1st request. The request context will be canceled while running the PostingsForMatchers().
	// At the time of the cancellation there's no other in-flight request, so we expect that the execution
	// is canceled and context.Canceled returned.
	_, err := c.PostingsForMatchers(reqCtx1, indexForPostingsMock{}, true, matchers...)
	require.ErrorIs(t, err, context.Canceled)

	wg.Wait()

	// The PostingsForMatchers() should have been called twice: the first time by the 1st request which is
	// canceled, and then again by the 2nd request that will run successfully.
	require.Equal(t, int32(2), callsCount.Load())

	// When the race condition is detected, we bypass the cache, and so we don't expect the result to be cached.
	require.Equal(t, 0, c.cached.Len())
}

func BenchmarkPostingsForMatchersCache(b *testing.B) {
	const (
		numMatchers = 100
		numPostings = 1000
	)

	var (
		ctx         = context.Background()
		indexReader = indexForPostingsMock{}
	)

	// Create some matchers.
	matchersLists := make([][]*labels.Matcher, numMatchers)
	for i := range matchersLists {
		matchersLists[i] = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "matchers", strconv.Itoa(i))}
	}

	// Create a postings list.
	refs := make([]storage.SeriesRef, numPostings)
	for r := range refs {
		refs[r] = storage.SeriesRef(r)
	}

	b.Run("no evictions", func(b *testing.B) {
		// Configure the cache to never evict.
		cache := NewPostingsForMatchersCache(time.Hour, 1000000, 1024*1024*1024, true)
		cache.postingsForMatchers = func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			return index.NewListPostings(refs), nil
		}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			_, err := cache.PostingsForMatchers(ctx, indexReader, true, matchersLists[n%len(matchersLists)]...)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("high eviction rate", func(b *testing.B) {
		// Configure the cache to evict continuously.
		cache := NewPostingsForMatchersCache(time.Hour, 0, 0, true)
		cache.postingsForMatchers = func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			return index.NewListPostings(refs), nil
		}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			_, err := cache.PostingsForMatchers(ctx, indexReader, true, matchersLists[n%len(matchersLists)]...)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

func BenchmarkPostingsForMatchersCache_ConcurrencyOnHighEvictionRate(b *testing.B) {
	const (
		numMatchers = 100
		numWorkers  = 100
	)

	var (
		ctx          = context.Background()
		indexReader  = indexForPostingsMock{}
		start        = make(chan struct{})
		workersErrMx = sync.Mutex{}
		workersErr   error
	)

	// Create some matchers.
	matchersLists := make([][]*labels.Matcher, numMatchers)
	for i := range matchersLists {
		matchersLists[i] = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "matchers", strconv.Itoa(i))}
	}

	// Create a postings list. In this test every postings lookup returns a cope of the same list.
	refs := make([]storage.SeriesRef, 10)
	for r := range refs {
		refs[r] = storage.SeriesRef(r)
	}

	// Configure the cache to evict continuously.
	cache := NewPostingsForMatchersCache(time.Hour, 0, 0, true)
	cache.postingsForMatchers = func(ctx context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
		return index.NewListPostings(refs), nil
	}

	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	for r := 0; r < numWorkers; r++ {
		go func() {
			defer wg.Done()

			// Wait until the start signal.
			<-start

			for n := 0; n < b.N/numWorkers; n++ {
				_, err := cache.PostingsForMatchers(ctx, indexReader, true, matchersLists[n%len(matchersLists)]...)
				if err != nil {
					workersErrMx.Lock()
					workersErr = err
					workersErrMx.Unlock()

					break
				}
			}
		}()
	}

	b.ResetTimer()

	// Start workers and wait until they're done.
	close(start)
	wg.Wait()

	require.NoError(b, workersErr)
}

type indexForPostingsMock struct{}

func (idx indexForPostingsMock) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	panic("implement me")
}

func (idx indexForPostingsMock) LabelValues(context.Context, string, ...*labels.Matcher) ([]string, error) {
	panic("implement me")
}

func (idx indexForPostingsMock) Postings(context.Context, string, ...string) (index.Postings, error) {
	panic("implement me")
}

func (idx indexForPostingsMock) PostingsForLabelMatching(context.Context, string, func(value string) bool) index.Postings {
	panic("implement me")
}

// timeNowMock offers a mockable time.Now() implementation
// empty value is ready to be used, and it should not be copied (use a reference).
type timeNowMock struct {
	sync.Mutex
	now time.Time
}

// timeNow can be used as a mocked replacement for time.Now().
func (t *timeNowMock) timeNow() time.Time {
	t.Lock()
	defer t.Unlock()
	if t.now.IsZero() {
		t.now = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	}
	return t.now
}

// advance advances the mocked time.Now() value.
func (t *timeNowMock) advance(d time.Duration) {
	t.Lock()
	defer t.Unlock()
	if t.now.IsZero() {
		t.now = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	}
	t.now = t.now.Add(d)
}

func BenchmarkMatchersKey(b *testing.B) {
	const totalMatchers = 10
	const matcherSets = 100
	sets := make([][]*labels.Matcher, matcherSets)
	for i := 0; i < matcherSets; i++ {
		for j := 0; j < totalMatchers; j++ {
			sets[i] = append(sets[i], labels.MustNewMatcher(labels.MatchType(j%4), fmt.Sprintf("%d_%d", i*13, j*65537), fmt.Sprintf("%x_%x", i*127, j*2_147_483_647)))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = matchersKey(sets[i%matcherSets])
	}
}

func TestContextsTracker(t *testing.T) {
	t.Run("1 tracked context, tracked context doesn't get canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := newContextsTracker()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		require.NoError(t, tracker.add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		tracker.close()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("1 tracked context, tracked context gets canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := newContextsTracker()
		t.Cleanup(tracker.close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("2 tracked contexts, only 1 tracked context gets canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := newContextsTracker()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		require.NoError(t, tracker.add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		tracker.close()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("2 tracked contexts, both contexts get canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := newContextsTracker()
		t.Cleanup(tracker.close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx2, cancelCtx2 := context.WithCancel(context.Background())
		require.NoError(t, tracker.add(ctx2))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx2()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("2 tracked contexts, 1 context gets canceled, the other context has deadline exceeded", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := newContextsTracker()
		t.Cleanup(tracker.close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx1, cancelCtx1 := context.WithCancel(context.Background())
		require.NoError(t, tracker.add(ctx1))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx2, cancelCtx2 := context.WithDeadline(context.Background(), time.Now())
		t.Cleanup(cancelCtx2)
		require.NoError(t, tracker.add(ctx2))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx1()

		// We expect the 2nd context deadline to expire immediately.
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("add() context to tracker but the tracker has been explicitly closed", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := newContextsTracker()
		t.Cleanup(tracker.close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		require.NoError(t, tracker.add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		tracker.close()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)

		require.Equal(t, errContextsTrackerClosed{}, tracker.add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("add() context to tracker but the tracker has been implicitly closed because all tracked contexts have been canceled", func(t *testing.T) {
		t.Parallel()

		tracker, execCtx := newContextsTracker()
		t.Cleanup(tracker.close)

		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		ctx, cancelCtx := context.WithCancel(context.Background())
		require.NoError(t, tracker.add(ctx))
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		cancelCtx()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)

		require.Equal(t, errContextsTrackerCanceled{}, tracker.add(context.Background()))
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})
}

func TestContextsTracker_Concurrency(t *testing.T) {
	const numContexts = 100

	t.Run("concurrently add and then cancel tracked contexts except 1", func(t *testing.T) {
		tracker, execCtx := newContextsTracker()
		t.Cleanup(tracker.close)

		cancels := make([]context.CancelFunc, 0, numContexts)

		// Concurrently add() concurrently.
		addWg := sync.WaitGroup{}
		addWg.Add(numContexts)

		for i := 0; i < numContexts; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)

			go func(ctx context.Context) {
				defer addWg.Done()
				tracker.add(ctx)
			}(ctx)
		}

		// Wait until all add() functions have done.
		addWg.Wait()

		// Concurrently cancel all contexts minus 1.
		cancelWg := sync.WaitGroup{}
		cancelWg.Add(numContexts - 1)

		for i := 0; i < numContexts-1; i++ {
			go func(i int) {
				defer cancelWg.Done()
				cancels[i]()
			}(i)
		}

		// Wait until contexts have been canceled.
		cancelWg.Wait()

		// Since we canceled all contexts minus 1, we expect the execution context hasn't been canceled yet.
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		// Cancel the last context.
		cancels[numContexts-1]()
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("concurrently add and then cancel tracked contexts", func(t *testing.T) {
		tracker, execCtx := newContextsTracker()
		t.Cleanup(tracker.close)

		cancels := make([]context.CancelFunc, 0, numContexts)

		// Concurrently add() concurrently.
		addWg := sync.WaitGroup{}
		addWg.Add(numContexts)

		for i := 0; i < numContexts; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)

			go func(ctx context.Context) {
				defer addWg.Done()
				tracker.add(ctx)
			}(ctx)
		}

		// Wait until all add() functions have done.
		addWg.Wait()

		// Since we haven't canceled any tracked context, we don't expect the execution context has been canceled neither.
		requireContextsTrackerExecutionContextDone(t, execCtx, false)

		// Concurrently cancel all contexts.
		cancelWg := sync.WaitGroup{}
		cancelWg.Add(numContexts)

		for i := 0; i < numContexts; i++ {
			go func(i int) {
				defer cancelWg.Done()
				cancels[i]()
			}(i)
		}

		// Wait until contexts have been canceled.
		cancelWg.Wait()

		// Since we canceled all contexts, we expect the execution context has been canceled too.
		requireContextsTrackerExecutionContextDone(t, execCtx, true)
	})

	t.Run("concurrently close tracker", func(t *testing.T) {
		tracker, execCtx := newContextsTracker()

		for i := 0; i < numContexts; i++ {
			tracker.add(context.Background())
		}

		// Concurrently close tracker.
		fire := make(chan struct{})

		closeWg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			closeWg.Add(1)

			go func() {
				defer closeWg.Done()

				<-fire
				tracker.close()
			}()
		}

		requireContextsTrackerExecutionContextDone(t, execCtx, false)
		close(fire)
		requireContextsTrackerExecutionContextDone(t, execCtx, true)

		// Wait until all gouroutines have done.
		closeWg.Wait()
	})
}

func requireContextsTrackerExecutionContextDone(t *testing.T, ctx context.Context, expected bool) {
	t.Helper()

	// The contextsTracker execution context is NOT cancelled synchronously once the tracked
	// contexts have done, so we allow for a short delay.
	select {
	case <-time.After(100 * time.Millisecond):
		if expected {
			t.Fatal("expected contextsTracker execution context to be done")
		}

	case <-ctx.Done():
		if !expected {
			t.Fatal("expected contextsTracker execution context to be not done")
		}
	}
}

func requirePostingsForMatchesCachePromiseTrackedContexts(t *testing.T, cache *PostingsForMatchersCache, cacheKey string, expected int) {
	t.Helper()

	require.Eventually(t, func() bool {
		promise, ok := cache.calls.Load(cacheKey)
		if !ok {
			return false
		}

		tracker := promise.(*postingsForMatcherPromise).callersCtxTracker
		return tracker.trackedContextsCount() == expected
	}, time.Second, 10*time.Millisecond)
}

func TestErrContextsTrackerClosed(t *testing.T) {
	err := errContextsTrackerClosed{}
	require.ErrorIs(t, err, errContextsTrackerClosed{})
	require.NotErrorIs(t, context.Canceled, errContextsTrackerClosed{})
	require.Equal(t, 0, size.Of(err))
}

func TestErrContextsTrackerCanceled(t *testing.T) {
	err := errContextsTrackerCanceled{}
	require.ErrorIs(t, err, errContextsTrackerCanceled{})
	require.NotErrorIs(t, context.Canceled, errContextsTrackerCanceled{})
	require.Equal(t, 0, size.Of(err))
}
