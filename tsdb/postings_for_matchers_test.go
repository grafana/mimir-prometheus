package tsdb

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

func TestPromisePostingsForMatchersProvider_PostingsForMatchers(t *testing.T) {
	// newPromisePostingsForMatchersProvider tests the NewPostingsForMatchersProvider constructor, but overrides the postingsForMatchers func
	newPromisePostingsForMatchersProvider := func(ttl time.Duration, pfm func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error), timeMock *timeNowMock) PostingsForMatchersProviderImpl {
		ifpMock := indexForPostingsMock{}
		p := NewPostingsForMatchersProvider(ttl).WithIndex(ifpMock)
		if p.postingsForMatchers == nil {
			t.Fatalf("NewPostingsForMatchersProvider() didn't assign postingsForMatchers func")
		}
		p.postingsForMatchers = pfm
		p.timeNow = timeMock.timeNow
		return p
	}

	t.Run("happy case one call", func(t *testing.T) {
		for _, concurrent := range []bool{true, false} {
			t.Run(fmt.Sprintf("concurrent=%t", concurrent), func(t *testing.T) {
				expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
				expectedPostingsErr := fmt.Errorf("failed successfully")

				p := newPromisePostingsForMatchersProvider(defaultPostingsForMatchersCacheTTL, func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
					require.IsType(t, indexForPostingsMock{}, ifp, "Incorrect IndexForPostings was provided to PostingsForMatchers, expected the mock, was given %v (%T)", ifp, ifp)
					require.Equal(t, expectedMatchers, ms, "Wrong label matchers provided, expected %v, got %v", expectedMatchers, ms)
					return index.ErrPostings(expectedPostingsErr), nil
				}, &timeNowMock{})
				t.Cleanup(func() { p.Close() })

				got, err := p.PostingsForMatchers(concurrent, expectedMatchers...)
				require.NoError(t, err)
				require.NotNil(t, got)
				require.Equal(t, got.Err(), expectedPostingsErr, "Expected ErrPostings with err %q, got %T with err %q", expectedPostingsErr, got, got.Err())
			})
		}
	})

	t.Run("err returned", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
		expectedErr := fmt.Errorf("failed successfully")

		p := newPromisePostingsForMatchersProvider(defaultPostingsForMatchersCacheTTL, func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
			return nil, expectedErr
		}, &timeNowMock{})
		t.Cleanup(func() { p.Close() })

		_, err := p.PostingsForMatchers(true, expectedMatchers...)
		require.Equal(t, expectedErr, err)
	})

	t.Run("happy case multiple concurrent calls: two same one different", func(t *testing.T) {
		for _, cacheEnabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("cacheEnabled=%t", cacheEnabled), func(t *testing.T) {
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

				expectedPostingsForMatchersCalls := 5
				// we'll block all the calls until we receive the exact amount. if we receive more, WaitGroup will panic
				called := make(chan struct{}, expectedPostingsForMatchersCalls)
				release := make(chan struct{})
				var ttl time.Duration
				if cacheEnabled {
					ttl = defaultPostingsForMatchersCacheTTL
				}
				p := newPromisePostingsForMatchersProvider(ttl, func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
					select {
					case called <- struct{}{}:
					default:
					}
					<-release
					return nil, fmt.Errorf(matchersString(ms))
				}, &timeNowMock{})
				t.Cleanup(func() { p.Close() })

				results := make([]string, len(calls))
				resultsWg := sync.WaitGroup{}
				resultsWg.Add(len(calls))

				// perform all calls
				for i := 0; i < len(calls); i++ {
					go func(i int) {
						_, err := p.PostingsForMatchers(true, calls[i]...)
						results[i] = err.Error()
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
					require.Equal(t, matchersString(c), results[i], "Call %d should have returned error %q, but got %q instead", i, matchersString(c), results[i])
				}
			})
		}
	})

	t.Run("with concurrent==false, result is not cached", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}

		var call int
		p := newPromisePostingsForMatchersProvider(defaultPostingsForMatchersCacheTTL, func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, &timeNowMock{})
		t.Cleanup(func() { p.Close() })

		// first call, fills the cache
		got, err := p.PostingsForMatchers(false, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, got.Err(), "result from call 1")

		// second call within the ttl (we didn't advance the time), should call again because concurrent==false
		got, err = p.PostingsForMatchers(false, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, got.Err(), "result from call 2")
	})

	t.Run("with cache disabled, result is not cached", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}

		var call int
		p := newPromisePostingsForMatchersProvider(0, func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, &timeNowMock{})
		t.Cleanup(func() { p.Close() })

		// first call, fills the cache
		got, err := p.PostingsForMatchers(true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, got.Err(), "result from call 1")

		// second call within the ttl (we didn't advance the time), should call again because concurrent==false
		got, err = p.PostingsForMatchers(true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, got.Err(), "result from call 2")
	})

	t.Run("cached value is returned, then it expires", func(t *testing.T) {
		timeNow := &timeNowMock{}
		expectedMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
		}

		var call int
		p := newPromisePostingsForMatchersProvider(defaultPostingsForMatchersCacheTTL, func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
			call++
			return index.ErrPostings(fmt.Errorf("result from call %d", call)), nil
		}, timeNow)
		t.Cleanup(func() { p.Close() })

		// first call, fills the cache
		got, err := p.PostingsForMatchers(true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, got.Err(), "result from call 1")

		timeNow.advance(defaultPostingsForMatchersCacheTTL / 2)

		// second call within the ttl, should use the cache
		got, err = p.PostingsForMatchers(true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, got.Err(), "result from call 1")

		timeNow.advance(defaultPostingsForMatchersCacheTTL / 2)

		// third call is after ttl (exactly), should call again
		got, err = p.PostingsForMatchers(true, expectedMatchers...)
		require.NoError(t, err)
		require.EqualError(t, got.Err(), "result from call 2")
	})
}

type indexForPostingsMock struct{}

func (idx indexForPostingsMock) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	panic("implement me")
}

func (idx indexForPostingsMock) Postings(name string, values ...string) (index.Postings, error) {
	panic("implement me")
}

// timeNowMock offers a mockable time.Now() implementation
// empty value is ready to be used, and it should not be copied (use a reference)
type timeNowMock struct {
	sync.RWMutex
	now time.Time
}

// timeNow can be used as a mocked replacement for time.Now()
func (t *timeNowMock) timeNow() time.Time {
	if t.now.IsZero() {
		t.now = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	}
	t.RLock()
	defer t.RUnlock()
	return t.now
}

// advance advances the mocked time.Now() value
func (t *timeNowMock) advance(d time.Duration) {
	if t.now.IsZero() {
		t.now = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	}
	t.Lock()
	defer t.Unlock()
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
