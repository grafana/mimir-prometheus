// Copyright 2021 Grafana Labs
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

func TestPostingsForMatchersCacheFactory(t *testing.T) {
	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			config := DefaultPostingsForMatchersCacheConfig
			config.Shared = shared
			config.KeyFunc = DefaultPostingsForMatchersCacheKeyFunc
			config.Invalidation = false
			config.CacheVersions = DefaultPostingsForMatchersCacheVersions
			config.TTL = time.Second
			config.MaxItems = 5
			config.MaxBytes = 500
			config.Force = false
			config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
			f := NewPostingsForMatchersCacheFactory(config)
			c1 := f.NewPostingsForMatchersCache(nil)
			c2 := f.NewPostingsForMatchersCache(nil)

			if shared {
				require.Same(t, c1, c2)
			} else {
				require.NotSame(t, c1, c2)
			}
		})
	}
}

func TestPostingsForMatchersCache(t *testing.T) {
	// newPostingsForMatchersCache tests the NewPostingsForMatcherCache constructor, but overrides the postingsForMatchers func
	newPostingsForMatchersCache := func(config PostingsForMatchersCacheConfig, pfm func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error), timeMock *timeNowMock) *PostingsForMatchersCache {
		c := NewPostingsForMatchersCacheFactory(config).NewPostingsForMatchersCache(nil)
		if c.postingsForMatchers == nil {
			t.Fatalf("NewPostingsForMatchersCache() didn't assign postingsForMatchers func")
		}
		c.postingsForMatchers = pfm
		c.timeNow = timeMock.timeNow
		return c
	}

	ctx := context.Background()

	t.Run("happy case one call", func(t *testing.T) {
		for _, shared := range []bool{true, false} {
			for _, invalidation := range []bool{true, false} {
				if !shared && invalidation {
					continue
				}
				for _, concurrent := range []bool{true, false} {
					t.Run(fmt.Sprintf("shared=%t, invalidation=%t,concurrent=%t", shared, invalidation, concurrent), func(t *testing.T) {
						expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo")}
						expectedPostingsErr := errors.New("failed successfully")
						reg := prometheus.NewRegistry()

						config := DefaultPostingsForMatchersCacheConfig
						config.Shared = shared
						config.Invalidation = invalidation
						config.TTL = DefaultPostingsForMatchersCacheTTL
						config.MaxItems = 5
						config.MaxBytes = 1000
						config.Force = false
						config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
						c := newPostingsForMatchersCache(config, func(_ context.Context, ix IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
							require.IsType(t, indexForPostingsMock{}, ix, "Incorrect IndexPostingsReader was provided to PostingsForMatchers, expected the mock, was given %v (%T)", ix, ix)
							require.Equal(t, expectedMatchers, ms, "Wrong label matchers provided, expected %v, got %v", expectedMatchers, ms)
							return index.ErrPostings(expectedPostingsErr), nil
						}, &timeNowMock{})

						p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, concurrent, headULID, expectedMatchers...)
						require.NoError(t, err)
						require.NotNil(t, p)
						require.Equal(t, expectedPostingsErr, p.Err(), "Expected ErrPostings with err %q, got %T with err %q", expectedPostingsErr, p, p.Err())

						expectedBytes := 0
						expectedEntries := 0
						expectedHits := 0
						expectedMisses := 0
						expectedSkips := 0
						if concurrent {
							if shared {
								expectedBytes = 244
							} else {
								expectedBytes = 215
							}
							expectedEntries = 1
							expectedMisses = 1
						} else {
							expectedSkips = 1
						}

						require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_bytes_total gauge
					postings_for_matchers_cache_bytes_total %d

					# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_entries_total gauge
					postings_for_matchers_cache_entries_total %d

					# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_requests_total counter
					postings_for_matchers_cache_requests_total 1

					# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_hits_total counter
					postings_for_matchers_cache_hits_total %d

					# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
					# TYPE postings_for_matchers_cache_invalidations_total counter
					postings_for_matchers_cache_invalidations_total 0

					# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
					# TYPE postings_for_matchers_cache_misses_total counter
					postings_for_matchers_cache_misses_total %d

					# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
					# TYPE postings_for_matchers_cache_skips_total counter
					postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
					postings_for_matchers_cache_skips_total{reason="ineligible"} %d
					postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

					# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_evictions_total counter
					postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
					postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
					postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
					postings_for_matchers_cache_evictions_total{reason="unknown"} 0
				`, expectedBytes, expectedEntries, expectedHits, expectedMisses, expectedSkips))))
					})
				}
			}
		}
	})

	t.Run("err returned", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		expectedErr := errors.New("failed successfully")
		reg := prometheus.NewRegistry()

		config := DefaultPostingsForMatchersCacheConfig
		config.MaxItems = 5
		config.MaxBytes = 1000
		config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
		c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
			return nil, expectedErr
		}, &timeNowMock{})

		_, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
		require.ErrorIs(t, err, expectedErr)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_bytes_total gauge
			postings_for_matchers_cache_bytes_total 186

			# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_entries_total gauge
			postings_for_matchers_cache_entries_total 1

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 1

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 0

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 0

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 1

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
			postings_for_matchers_cache_skips_total{reason="ineligible"} 0
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
	})

	t.Run("happy case multiple concurrent calls: two same one different", func(t *testing.T) {
		for _, shared := range []bool{true, false} {
			for _, invalidation := range []bool{true, false} {
				if !shared && invalidation {
					continue
				}
				for _, cacheEnabled := range []bool{true, false} {
					t.Run(fmt.Sprintf("shared=%t, invalidation=%t, cacheEnabled=%t", shared, invalidation, cacheEnabled), func(t *testing.T) {
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
								config := DefaultPostingsForMatchersCacheConfig
								config.Shared = shared
								config.Invalidation = invalidation
								config.TTL = ttl
								config.MaxItems = 5
								config.MaxBytes = 1000
								config.Force = forced
								c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
									select {
									case called <- struct{}{}:
									default:
									}
									<-release
									return nil, fmt.Errorf("%s", matchersString(ms))
								}, &timeNowMock{})

								results := make([]error, len(calls))
								resultsWg := sync.WaitGroup{}
								resultsWg.Add(len(calls))

								// perform all calls
								for i := 0; i < len(calls); i++ {
									go func(i int) {
										_, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, concurrent, headULID, calls[i]...)
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
			}
		}
	})

	t.Run("with concurrent==false and force==false, result is not cached", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		reg := prometheus.NewRegistry()

		var call int
		config := DefaultPostingsForMatchersCacheConfig
		config.MaxItems = 5
		config.MaxBytes = 1000
		config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
		c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, &timeNowMock{})

		// first call, fills the cache
		p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, false, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// second call within the ttl (we didn't advance the time), should call again because concurrent==false
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, false, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_bytes_total gauge
			postings_for_matchers_cache_bytes_total 0

			# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_entries_total gauge
			postings_for_matchers_cache_entries_total 0

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 2

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 0

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 0

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 0

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
			postings_for_matchers_cache_skips_total{reason="ineligible"} 2
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
	})

	t.Run("with cache disabled, result is not cached", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		reg := prometheus.NewRegistry()

		var call int
		config := DefaultPostingsForMatchersCacheConfig
		config.TTL = 0
		config.MaxItems = 1000
		config.MaxBytes = 1000
		config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
		c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, &timeNowMock{})

		// first call, fills the cache
		p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// second call within the ttl (we didn't advance the time), should call again because concurrent==false
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_bytes_total gauge
			postings_for_matchers_cache_bytes_total 0

			# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_entries_total gauge
			postings_for_matchers_cache_entries_total 0

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 2

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 0

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 0

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 2

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
			postings_for_matchers_cache_skips_total{reason="ineligible"} 0
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
	})

	t.Run("cached value is returned, then it expires", func(t *testing.T) {
		for _, shared := range []bool{true, false} {
			for _, invalidation := range []bool{true, false} {
				if !shared && invalidation {
					continue
				}
				t.Run(fmt.Sprintf("shared=%t, invalidation=%t", shared, invalidation), func(t *testing.T) {
					timeNow := &timeNowMock{}
					expectedMatchers := []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo"),
					}
					reg := prometheus.NewRegistry()

					var call int
					config := DefaultPostingsForMatchersCacheConfig
					config.Shared = shared
					config.Invalidation = invalidation
					config.MaxItems = 5
					config.MaxBytes = 1000
					config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
					c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
						call++
						return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
					}, timeNow)

					// first call, fills the cache
					p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
					require.NoError(t, err)
					require.EqualError(t, p.Err(), "result from call 1")

					timeNow.advance(DefaultPostingsForMatchersCacheTTL / 2)

					// second call within the ttl, should use the cache
					p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
					require.NoError(t, err)
					require.EqualError(t, p.Err(), "result from call 1")

					timeNow.advance(DefaultPostingsForMatchersCacheTTL / 2)

					// third call is after ttl (exactly), should call again
					p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
					require.NoError(t, err)
					require.EqualError(t, p.Err(), "result from call 2")

					expectedBytes := 0
					expectedEntries := 1
					if shared {
						expectedBytes = 243
					} else {
						expectedBytes = 214
					}

					require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_bytes_total gauge
					postings_for_matchers_cache_bytes_total %d

					# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_entries_total gauge
					postings_for_matchers_cache_entries_total %d

					# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_requests_total counter
					postings_for_matchers_cache_requests_total 3

					# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_hits_total counter
					postings_for_matchers_cache_hits_total 1

					# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
					# TYPE postings_for_matchers_cache_invalidations_total counter
					postings_for_matchers_cache_invalidations_total 0

					# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
					# TYPE postings_for_matchers_cache_misses_total counter
					postings_for_matchers_cache_misses_total 2

					# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
					# TYPE postings_for_matchers_cache_skips_total counter
					postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
					postings_for_matchers_cache_skips_total{reason="ineligible"} 0
					postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

					# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_evictions_total counter
					postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
					postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
					postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 1
					postings_for_matchers_cache_evictions_total{reason="unknown"} 0
				`, expectedBytes, expectedEntries))))
				})
			}
		}
	})

	t.Run("cached value is evicted because cache exceeds max items", func(t *testing.T) {
		for _, shared := range []bool{true, false} {
			for _, invalidation := range []bool{true, false} {
				if !shared && invalidation {
					continue
				}
				t.Run(fmt.Sprintf("shared=%t, invalidation=%t", shared, invalidation), func(t *testing.T) {
					const maxItems = 5

					timeNow := &timeNowMock{}
					reg := prometheus.NewRegistry()

					calls := make([][]*labels.Matcher, maxItems)
					for i := range calls {
						calls[i] = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", strconv.Itoa(i))}
					}

					callsPerMatchers := map[string]int{}
					config := DefaultPostingsForMatchersCacheConfig
					config.Shared = shared
					config.Invalidation = invalidation
					config.MaxItems = maxItems
					config.MaxBytes = 100000
					config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
					c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
						k := cacheKeyForMatchers(ms)
						callsPerMatchers[k]++
						return index.ErrPostings(fmt.Errorf("result from call %d", callsPerMatchers[k])), nil
					}, timeNow)

					// each one of the first maxItems calls is cached properly
					for _, matchers := range calls {
						// first call
						p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchers...)
						require.NoError(t, err)
						require.EqualError(t, p.Err(), "result from call 1")

						// cached value
						p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchers...)
						require.NoError(t, err)
						require.EqualError(t, p.Err(), "result from call 1")
					}

					// one extra call is made, which is cached properly, but evicts the first cached value
					someExtraMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo")}
					// first call
					p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, someExtraMatchers...)
					require.NoError(t, err)
					require.EqualError(t, p.Err(), "result from call 1")

					// cached value
					p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, someExtraMatchers...)
					require.NoError(t, err)
					require.EqualError(t, p.Err(), "result from call 1")

					// make first call again, it's calculated again
					p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, calls[0]...)
					require.NoError(t, err)
					require.EqualError(t, p.Err(), "result from call 2")

					expectedBytes := 0
					expectedEntries := 6
					if shared {
						expectedBytes = 1448
					} else {
						expectedBytes = 1274
					}

					require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_bytes_total gauge
					postings_for_matchers_cache_bytes_total %d

					# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_entries_total gauge
					postings_for_matchers_cache_entries_total %d

					# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_requests_total counter
					postings_for_matchers_cache_requests_total 13

					# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_hits_total counter
					postings_for_matchers_cache_hits_total 6

					# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
					# TYPE postings_for_matchers_cache_invalidations_total counter
					postings_for_matchers_cache_invalidations_total 0

					# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
					# TYPE postings_for_matchers_cache_misses_total counter
					postings_for_matchers_cache_misses_total 7

					# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
					# TYPE postings_for_matchers_cache_skips_total counter
					postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
					postings_for_matchers_cache_skips_total{reason="ineligible"} 0
					postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

					# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_evictions_total counter
					postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
					postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 1
					postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
					postings_for_matchers_cache_evictions_total{reason="unknown"} 0
				`, expectedBytes, expectedEntries))))
				})
			}
		}
	})

	t.Run("cached value is evicted because cache exceeds max bytes", func(t *testing.T) {
		for _, shared := range []bool{true, false} {
			for _, invalidation := range []bool{true, false} {
				if !shared && invalidation {
					continue
				}
				t.Run(fmt.Sprintf("shared=%t, invalidation=%t", shared, invalidation), func(t *testing.T) {
					const (
						maxItems         = 100 // Never hit it.
						maxBytes         = 1500
						numMatchers      = 5
						postingsListSize = 30 // 8 bytes per posting ref, so 30 x 8 = 240 bytes.
					)

					reg := prometheus.NewRegistry()

					// Generate some matchers.
					matchersLists := make([][]*labels.Matcher, numMatchers)
					for i := range matchersLists {
						matchersLists[i] = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", strconv.Itoa(i))}
					}

					// Generate some postings lists.
					refsLists := make(map[string][]storage.SeriesRef, numMatchers)
					for i := 0; i < numMatchers; i++ {
						refs := make([]storage.SeriesRef, postingsListSize)
						for r := range refs {
							refs[r] = storage.SeriesRef(i * r)
						}

						refsLists[cacheKeyForMatchers(matchersLists[i])] = refs
					}

					callsPerMatchers := map[string]int{}
					config := DefaultPostingsForMatchersCacheConfig
					config.Shared = shared
					config.Invalidation = invalidation
					config.MaxItems = maxItems
					config.MaxBytes = maxBytes
					config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
					c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
						k := cacheKeyForMatchers(ms)
						callsPerMatchers[k]++
						return index.NewListPostings(refsLists[k]), nil
					}, &timeNowMock{})

					// We expect to cache 3 items. So we're going to call PostingsForMatchers for 3 matchers
					// and then double check they're all cached. To do it, we iterate twice.
					for run := 0; run < 2; run++ {
						for i := 0; i < 3; i++ {
							matchers := matchersLists[i]

							p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchers...)
							require.NoError(t, err)

							actual, err := index.ExpandPostings(p)
							require.NoError(t, err)
							require.Equal(t, refsLists[cacheKeyForMatchers(matchers)], actual)
						}
					}

					// At this point we expect that the postings have been computed only once for the 3 matchers.
					for i := 0; i < 3; i++ {
						require.Equalf(t, 1, callsPerMatchers[cacheKeyForMatchers(matchersLists[i])], "matcher %d", i)
					}

					// Call PostingsForMatchers() for a 4th matcher. We expect this will evict the oldest cached entry.
					for run := 0; run < 2; run++ {
						matchers := matchersLists[3]

						p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchers...)
						require.NoError(t, err)

						actual, err := index.ExpandPostings(p)
						require.NoError(t, err)
						require.Equal(t, refsLists[cacheKeyForMatchers(matchers)], actual)
					}

					// To ensure the 1st (oldest) entry was removed, we call PostingsForMatchers() again on those matchers.
					_, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchersLists[0]...)
					require.NoError(t, err)

					require.Equal(t, 2, callsPerMatchers[cacheKeyForMatchers(matchersLists[0])])
					require.Equal(t, 1, callsPerMatchers[cacheKeyForMatchers(matchersLists[1])])
					require.Equal(t, 1, callsPerMatchers[cacheKeyForMatchers(matchersLists[2])])
					require.Equal(t, 1, callsPerMatchers[cacheKeyForMatchers(matchersLists[3])])

					expectedBytes := 0
					expectedEntries := 4
					if shared {
						expectedBytes = 1912
					} else {
						expectedBytes = 1796
					}

					require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_bytes_total gauge
					postings_for_matchers_cache_bytes_total %d

					# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_entries_total gauge
					postings_for_matchers_cache_entries_total %d

					# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_requests_total counter
					postings_for_matchers_cache_requests_total 9

					# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_hits_total counter
					postings_for_matchers_cache_hits_total 4

					# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
					# TYPE postings_for_matchers_cache_invalidations_total counter
					postings_for_matchers_cache_invalidations_total 0

					# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
					# TYPE postings_for_matchers_cache_misses_total counter
					postings_for_matchers_cache_misses_total 5

					# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
					# TYPE postings_for_matchers_cache_skips_total counter
					postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
					postings_for_matchers_cache_skips_total{reason="ineligible"} 0
					postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

					# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
					# TYPE postings_for_matchers_cache_evictions_total counter
					postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 1
					postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
					postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
					postings_for_matchers_cache_evictions_total{reason="unknown"} 0
				`, expectedBytes, expectedEntries))))
				})
			}
		}
	})

	t.Run("cached entry is invalidated", func(t *testing.T) {
		timeNow := &timeNowMock{}
		expectedMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo"),
		}
		reg := prometheus.NewRegistry()

		var call int
		config := DefaultPostingsForMatchersCacheConfig
		config.Shared = true
		config.Invalidation = true
		config.MaxItems = 10
		config.MaxBytes = 1000
		config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
		c := newPostingsForMatchersCache(config, func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, timeNow)

		// first call, misses
		p, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// second call, hits
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 1")

		// invalidate the metric
		c.InvalidateMetric("", "foo")

		// third call, misses
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")

		// fourth call, hits
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 2")

		// fifth call with non-head block misses
		p, err = c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, ulid.MustParse("0000000000XXXXXXXXXXXXXFOO"), expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, p.Err(), "result from call 3")

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_bytes_total gauge
			postings_for_matchers_cache_bytes_total 729

			# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_entries_total gauge
			postings_for_matchers_cache_entries_total 3

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 5

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 2

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 1

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 3

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
			postings_for_matchers_cache_skips_total{reason="ineligible"} 0
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
	})

	t.Run("initial request context is canceled, no other in-flight requests", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		expectedPostings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))
		reg := prometheus.NewRegistry()

		var reqCtx context.Context
		var cancelReqCtx context.CancelFunc
		callsCount := atomic.NewInt32(0)

		config := DefaultPostingsForMatchersCacheConfig
		config.TTL = time.Hour
		config.MaxItems = 5
		config.MaxBytes = 1000
		config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
		c := newPostingsForMatchersCache(config, func(ctx context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
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
				return expectedPostings.Clone(context.TODO()), nil
			}
		}, &timeNowMock{})

		// Run PostingsForMatchers() a first time, cancelling the context while it's executing.
		reqCtx, cancelReqCtx = context.WithCancel(context.Background())
		_, err := c.PostingsForMatchers(reqCtx, indexForPostingsMock{}, true, headULID, matchers...)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int32(1), callsCount.Load())

		// We expect the promise has not been cached.
		require.Equal(t, 0, c.cached.Len())

		// Run PostingsForMatchers() a second time, not cancelling the context. Since the previous
		// canceled execution was not cached, we expect to run it again and get the actual result.
		reqCtx, cancelReqCtx = context.Background(), nil
		actualPostings, err := c.PostingsForMatchers(reqCtx, indexForPostingsMock{}, true, headULID, matchers...)
		require.NoError(t, err)
		require.Equal(t, expectedPostings.Clone(context.TODO()), actualPostings)
		require.Equal(t, int32(2), callsCount.Load())

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_bytes_total gauge
			postings_for_matchers_cache_bytes_total 222

			# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_entries_total gauge
			postings_for_matchers_cache_entries_total 1

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 2

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 0

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 0

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 2

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
			postings_for_matchers_cache_skips_total{reason="ineligible"} 0
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
	})

	t.Run("initial request context is cancelled, second request is not cancelled", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		expectedPostings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))
		reg := prometheus.NewRegistry()

		reqCtx1, cancelReqCtx1 := context.WithCancel(context.Background())
		waitBeforeCancelReqCtx1 := make(chan struct{})
		callsCount := atomic.NewInt32(0)

		config := DefaultPostingsForMatchersCacheConfig
		config.TTL = time.Hour
		config.MaxItems = 5
		config.MaxBytes = 1000
		config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
		c := newPostingsForMatchersCache(config, func(ctx context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
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
				return expectedPostings.Clone(context.TODO()), nil
			}
		}, &timeNowMock{})

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()

			_, err := c.PostingsForMatchers(reqCtx1, indexForPostingsMock{}, true, headULID, matchers...)
			require.ErrorIs(t, err, context.Canceled)
		}()

		go func() {
			defer wg.Done()

			actualPostings, err := c.PostingsForMatchers(context.Background(), indexForPostingsMock{}, true, headULID, matchers...)
			require.NoError(t, err)
			require.Equal(t, expectedPostings.Clone(context.TODO()), actualPostings)
		}()

		// Wait until the 2nd request attaches to the 1st one.
		requirePostingsForMatchesCachePromiseTrackedContexts(t, c, cacheKeyForMatchers(matchers), 2)

		close(waitBeforeCancelReqCtx1)
		wg.Wait()

		// We expect the PostingsForMatchers has been called only once.
		require.Equal(t, int32(1), callsCount.Load())

		// We expect the promise has been cached.
		require.Equal(t, 1, c.cached.Len())

		// Send another request. Since the result was cached, we expect the result is getting picked up
		// from the cache.
		actualPostings, err := c.PostingsForMatchers(context.Background(), indexForPostingsMock{}, true, headULID, matchers...)
		require.NoError(t, err)
		require.Equal(t, expectedPostings.Clone(context.TODO()), actualPostings)
		require.Equal(t, int32(1), callsCount.Load())

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_bytes_total gauge
			postings_for_matchers_cache_bytes_total 222

			# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_entries_total gauge
			postings_for_matchers_cache_entries_total 1

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 3

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 2

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 0

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 1

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
			postings_for_matchers_cache_skips_total{reason="ineligible"} 0
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
	})

	t.Run("initial and subsequent requests are canceled during execution", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		postings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))
		reg := prometheus.NewRegistry()

		reqCtx1, cancelReqCtx1 := context.WithCancel(context.Background())
		reqCtx2, cancelReqCtx2 := context.WithCancel(context.Background())
		cancelRequests := make(chan struct{})
		callsCount := atomic.NewInt32(0)

		config := DefaultPostingsForMatchersCacheConfig
		config.TTL = time.Hour
		config.MaxItems = 5
		config.MaxBytes = 1000
		config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
		c := newPostingsForMatchersCache(config, func(ctx context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
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
				return postings.Clone(context.TODO()), nil
			}
		}, &timeNowMock{})

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()

			_, err := c.PostingsForMatchers(reqCtx1, indexForPostingsMock{}, true, headULID, matchers...)
			require.ErrorIs(t, err, context.Canceled)
		}()

		go func() {
			defer wg.Done()

			_, err := c.PostingsForMatchers(reqCtx2, indexForPostingsMock{}, true, headULID, matchers...)
			require.ErrorIs(t, err, context.Canceled)
		}()

		// Wait until the 2nd request attaches to the 1st one.
		requirePostingsForMatchesCachePromiseTrackedContexts(t, c, cacheKeyForMatchers(matchers), 2)

		close(cancelRequests)
		wg.Wait()

		// We expect the promise has not been cached.
		require.Equal(t, 0, c.cached.Len())

		require.Equal(t, int32(1), callsCount.Load())

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_bytes_total gauge
			postings_for_matchers_cache_bytes_total 0

			# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_entries_total gauge
			postings_for_matchers_cache_entries_total 0

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 2

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 1

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 0

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 1

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
			postings_for_matchers_cache_skips_total{reason="ineligible"} 0
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
	})
}

func TestPostingsForMatchersCache_ShouldNotReturnStaleEntriesWhileAnotherGoroutineIsEvictingTheCache(t *testing.T) {
	var (
		matchers   = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		ttl        = time.Second
		ctx        = context.Background()
		nextCallID = atomic.NewUint64(0)
		reg        = prometheus.NewRegistry()
		timeMock   = &timeNowMock{}
	)

	config := DefaultPostingsForMatchersCacheConfig
	config.CacheVersions = 0
	config.TTL = ttl
	config.MaxItems = 1000
	config.MaxBytes = 1024 * 1024
	config.Force = true
	config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
	c := NewPostingsForMatchersCacheFactory(config).NewPostingsForMatchersCache(nil)
	c.timeNow = timeMock.timeNow

	// Issue a first call to cache the postings.
	c.postingsForMatchers = func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
		return index.ErrPostings(fmt.Errorf("result from call %d", nextCallID.Inc())), nil
	}

	postings, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchers...)
	require.NoError(t, err)
	require.EqualError(t, postings.Err(), "result from call 1")

	// Progress time to make the TTL expiring.
	timeMock.advance(ttl + 1)

	// Run 2 concurrent requests. One will expire the items from the cache, while the other one will
	// skip the expiration check. We expect none of them to return the stale entry from the cache.
	firstCall := atomic.NewBool(true)
	firstCallReceived := make(chan struct{})

	c.evictHeadBeforeHook = func() {
		// Wait until the other goroutine called the downstream PostingsForMatchers() because the cached entry is expired
		// even if it has not been removed from the cache yet.
		select {
		case <-firstCallReceived:
		case <-time.After(5 * time.Second):
			// Do not block forever. The test will fail anyway in this case.
		}
	}

	c.postingsForMatchers = func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
		if firstCall.CompareAndSwap(true, false) {
			close(firstCallReceived)
		}

		return index.ErrPostings(fmt.Errorf("result from call %d", nextCallID.Inc())), nil
	}

	results := make([]string, 2)
	callersWg := sync.WaitGroup{}
	callersWg.Add(2)

	go func() {
		defer callersWg.Done()

		postings, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchers...)
		require.NoError(t, err)
		results[0] = postings.Err().Error()
	}()

	go func() {
		defer callersWg.Done()

		postings, err := c.PostingsForMatchers(ctx, indexForPostingsMock{}, true, headULID, matchers...)
		require.NoError(t, err)
		results[1] = postings.Err().Error()
	}()

	callersWg.Wait()

	require.ElementsMatch(t, []string{"result from call 2", "result from call 3"}, results)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
		# TYPE postings_for_matchers_cache_bytes_total gauge
		postings_for_matchers_cache_bytes_total 209

		# HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
		# TYPE postings_for_matchers_cache_entries_total gauge
		postings_for_matchers_cache_entries_total 1

		# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
		# TYPE postings_for_matchers_cache_requests_total counter
		postings_for_matchers_cache_requests_total 3

		# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
		# TYPE postings_for_matchers_cache_hits_total counter
		postings_for_matchers_cache_hits_total 0

		# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
		# TYPE postings_for_matchers_cache_invalidations_total counter
		postings_for_matchers_cache_invalidations_total 0

		# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
		# TYPE postings_for_matchers_cache_misses_total counter
		postings_for_matchers_cache_misses_total 2

		# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
		# TYPE postings_for_matchers_cache_skips_total counter
		postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 0
		postings_for_matchers_cache_skips_total{reason="ineligible"} 0
		postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 1

		# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
		# TYPE postings_for_matchers_cache_evictions_total counter
		postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
		postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
		postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 1
		postings_for_matchers_cache_evictions_total{reason="unknown"} 0
	`)))
}

func TestPostingsForMatchersCache_RaceConditionBetweenExecutionContextCancellationAndNewRequest(t *testing.T) {
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	expectedPostings := index.NewPostingsCloner(index.NewListPostings([]storage.SeriesRef{1, 2, 3}))
	reg := prometheus.NewRegistry()

	reqCtx1, cancelReqCtx1 := context.WithCancel(context.Background())
	callsCount := atomic.NewInt32(0)

	wg := sync.WaitGroup{}
	wg.Add(1)

	config := DefaultPostingsForMatchersCacheConfig
	config.CacheVersions = 0
	config.TTL = time.Hour
	config.MaxItems = 5
	config.MaxBytes = 1000
	config.Force = true
	config.Metrics = NewPostingsForMatchersCacheMetrics(reg)
	c := NewPostingsForMatchersCacheFactory(config).NewPostingsForMatchersCache(nil)

	c.postingsForMatchers = func(ctx context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
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
			return expectedPostings.Clone(context.TODO()), nil
		}
	}

	c.onPromiseExecutionDoneBeforeHook = func() {
		// When the 1st request has been canceled, but before the promise is removed from the calls map,
		// we do issue a 2nd request. The 2nd request is expected to succeed, because its context has
		// not been canceled.
		go func() {
			defer wg.Done()

			actualPostings, err := c.PostingsForMatchers(context.Background(), indexForPostingsMock{}, true, headULID, matchers...)
			require.NoError(t, err)
			require.Equal(t, expectedPostings.Clone(context.TODO()), actualPostings)
		}()

		// Wait a (more than) reasonable amount of time to let the 2nd request to kick off.
		time.Sleep(time.Second)
	}

	// Issue the 1st request. The request context will be canceled while running the PostingsForMatchers().
	// At the time of the cancellation there's no other in-flight request, so we expect that the execution
	// is canceled and context.Canceled returned.
	_, err := c.PostingsForMatchers(reqCtx1, indexForPostingsMock{}, true, headULID, matchers...)
	require.ErrorIs(t, err, context.Canceled)

	wg.Wait()

	// The PostingsForMatchers() should have been called twice: the first time by the 1st request which is
	// canceled, and then again by the 2nd request that will run successfully.
	require.Equal(t, int32(2), callsCount.Load())

	// When the race condition is detected, we bypass the cache, and so we don't expect the result to be cached.
	require.Equal(t, 0, c.cached.Len())

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		    # HELP postings_for_matchers_cache_bytes_total Total number of bytes in the PostingsForMatchers cache.
		    # TYPE postings_for_matchers_cache_bytes_total gauge
		    postings_for_matchers_cache_bytes_total 0

		    # HELP postings_for_matchers_cache_entries_total Total number of entries in the PostingsForMatchers cache.
		    # TYPE postings_for_matchers_cache_entries_total gauge
		    postings_for_matchers_cache_entries_total 0

			# HELP postings_for_matchers_cache_requests_total Total number of requests to the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_requests_total counter
			postings_for_matchers_cache_requests_total 2

			# HELP postings_for_matchers_cache_hits_total Total number of postings lists returned from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_hits_total counter
			postings_for_matchers_cache_hits_total 0

			# HELP postings_for_matchers_cache_invalidations_total Total number of cache entries that were invalidated.
			# TYPE postings_for_matchers_cache_invalidations_total counter
			postings_for_matchers_cache_invalidations_total 0

			# HELP postings_for_matchers_cache_misses_total Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.
			# TYPE postings_for_matchers_cache_misses_total counter
			postings_for_matchers_cache_misses_total 1

			# HELP postings_for_matchers_cache_skips_total Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.
			# TYPE postings_for_matchers_cache_skips_total counter
			postings_for_matchers_cache_skips_total{reason="canceled-cached-entry"} 1
			postings_for_matchers_cache_skips_total{reason="ineligible"} 0
			postings_for_matchers_cache_skips_total{reason="stale-cached-entry"} 0

			# HELP postings_for_matchers_cache_evictions_total Total number of evictions from the PostingsForMatchers cache.
			# TYPE postings_for_matchers_cache_evictions_total counter
			postings_for_matchers_cache_evictions_total{reason="max-bytes-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="max-items-reached"} 0
			postings_for_matchers_cache_evictions_total{reason="ttl-expired"} 0
			postings_for_matchers_cache_evictions_total{reason="unknown"} 0
		`)))
}

func Test_sharedCacheKey(t *testing.T) {
	ms := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "bar", "baz"),
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo"),
	}

	key := sharedCacheKey(0, "user", headULID, ms)
	require.Equal(t, "0\u0000user\u00000000000000XXXXXXXXXXXXHEAD\u0000__name__=foo\u0000bar=baz", key)
}

func TestMetricVersions(t *testing.T) {
	createFn := func() *metricVersions {
		return &metricVersions{
			versions: make([]atomic.Int64, 100),
		}
	}

	t.Run("empty strings should be supported", func(t *testing.T) {
		mv := createFn()
		require.Equal(t, 0, mv.getVersion("", ""))
		mv.incrementVersion("", "")
		require.Equal(t, 1, mv.getVersion("", ""))
	})

	t.Run("initial version should be 0", func(t *testing.T) {
		mv := createFn()
		require.Equal(t, 0, mv.getVersion("user1", "foo"))
	})

	t.Run("versions should increment", func(t *testing.T) {
		mv := createFn()
		mv.incrementVersion("user1", "foo")
		mv.incrementVersion("user1", "foo")
		require.Equal(t, 2, mv.getVersion("user1", "foo"))
	})

	t.Run("versioned should be isolated by user", func(t *testing.T) {
		mv := createFn()
		mv.incrementVersion("user1", "foo")
		require.Equal(t, 1, mv.getVersion("user1", "foo"))
		require.Equal(t, 0, mv.getVersion("user2", "foo"))
	})

	t.Run("versioned should be isolated by metric name", func(t *testing.T) {
		mv := createFn()
		mv.incrementVersion("user1", "foo")
		require.Equal(t, 1, mv.getVersion("user1", "foo"))
		require.Equal(t, 0, mv.getVersion("user1", "bar"))
	})
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

	for _, shared := range []bool{true, false} {
		for _, invalidation := range []bool{true, false} {
			if !shared && invalidation {
				continue
			}
			b.Run(fmt.Sprintf("no evictions, shared=%t, invalidation=%t", shared, invalidation), func(b *testing.B) {
				// Configure the cache to never evict.
				config := DefaultPostingsForMatchersCacheConfig
				config.Shared = shared
				config.Invalidation = invalidation
				config.TTL = time.Hour
				config.MaxItems = 1000000
				config.MaxBytes = 1024 * 1024 * 1024
				config.Force = true
				config.Metrics = NewPostingsForMatchersCacheMetrics(nil)
				cache := NewPostingsForMatchersCacheFactory(config).NewPostingsForMatchersCache(nil)
				cache.postingsForMatchers = func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
					return index.NewListPostings(refs), nil
				}

				b.ResetTimer()

				for n := 0; n < b.N; n++ {
					_, err := cache.PostingsForMatchers(ctx, indexReader, true, headULID, matchersLists[n%len(matchersLists)]...)
					if err != nil {
						b.Fatalf("unexpected error: %v", err)
					}
				}
			})

			b.Run(fmt.Sprintf("high eviction rate, shared=%t, invalidation=%t", shared, invalidation), func(b *testing.B) {
				// Configure the cache to evict continuously.
				config := DefaultPostingsForMatchersCacheConfig
				config.Shared = shared
				config.Invalidation = invalidation
				config.TTL = time.Hour
				config.MaxItems = 0
				config.MaxBytes = 0
				config.Force = true
				config.Metrics = NewPostingsForMatchersCacheMetrics(nil)
				cache := NewPostingsForMatchersCacheFactory(config).NewPostingsForMatchersCache(nil)
				cache.postingsForMatchers = func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
					return index.NewListPostings(refs), nil
				}

				b.ResetTimer()

				for n := 0; n < b.N; n++ {
					_, err := cache.PostingsForMatchers(ctx, indexReader, true, headULID, matchersLists[n%len(matchersLists)]...)
					if err != nil {
						b.Fatalf("unexpected error: %v", err)
					}
				}
			})
		}
	}
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
	config := DefaultPostingsForMatchersCacheConfig
	config.CacheVersions = 0
	config.TTL = time.Hour
	config.MaxItems = 0
	config.MaxBytes = 0
	config.Force = true
	config.Metrics = NewPostingsForMatchersCacheMetrics(nil)
	cache := NewPostingsForMatchersCacheFactory(config).NewPostingsForMatchersCache(nil)
	cache.postingsForMatchers = func(_ context.Context, _ IndexPostingsReader, _ ...*labels.Matcher) (index.Postings, error) {
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
				_, err := cache.PostingsForMatchers(ctx, indexReader, true, headULID, matchersLists[n%len(matchersLists)]...)
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

func (indexForPostingsMock) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, error) {
	panic("implement me")
}

func (indexForPostingsMock) Postings(context.Context, string, ...string) (index.Postings, error) {
	panic("implement me")
}

func (indexForPostingsMock) PostingsForLabelMatching(context.Context, string, func(value string) bool) index.Postings {
	panic("implement me")
}

func (indexForPostingsMock) PostingsForAllLabelValues(context.Context, string) index.Postings {
	panic("implement me")
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

	b.Run("non-shared cache key", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = cacheKeyForMatchers(sets[i%matcherSets])
		}
	})

	b.Run("shared cache key", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sharedCacheKey(1, "foo", headULID, sets[i%matcherSets])
		}
	})
}

func requirePostingsForMatchesCachePromiseTrackedContexts(t *testing.T, cache *PostingsForMatchersCache, cacheKey string, expected int) {
	t.Helper()

	require.Eventually(t, func() bool {
		promise, ok := cache.calls.Load(cacheKey)
		if !ok {
			return false
		}

		tracker := promise.(*postingsForMatcherPromise).callersCtxTracker
		return tracker.TrackedContextsCount() == expected
	}, time.Second, 10*time.Millisecond)
}
