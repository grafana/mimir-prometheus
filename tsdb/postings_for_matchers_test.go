package tsdb

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

func TestPromisePostingsForMatchersProvider_PostingsForMatchers(t *testing.T) {
	// newPromisePostingsForMatchersProvider tests the NewPostingsForMatchersProvider constructor, but overrides the postingsForMatchers func
	newPromisePostingsForMatchersProvider := func(pfm func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error)) PostingsForMatchersProviderImpl {
		ifpMock := indexForPostingsMock{}
		p := NewPostingsForMatchersProvider(defaultPostingsForMatchersCacheTTL).WithIndex(ifpMock)
		if p.postingsForMatchers == nil {
			t.Fatalf("NewPostingsForMatchersProvider() didn't assign postingsForMatchers func")
		}
		p.postingsForMatchers = pfm
		return p
	}

	t.Run("happy case one call", func(t *testing.T) {
		for _, concurrent := range []bool{true, false} {
			t.Run(fmt.Sprintf("concurrent=%t", concurrent), func(t *testing.T) {
				expectedMatchers := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				}
				expectedPostingsErr := fmt.Errorf("failed successfully")

				p := newPromisePostingsForMatchersProvider(func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
					if _, mocked := ifp.(indexForPostingsMock); !mocked {
						t.Fatalf("Incorrect IndexForPostings was provided to PostingsForMatchers, expected the mock, was given %v (%T)", ifp, ifp)
					}
					if len(ms) != 1 || ms[0].Name != "foo" {
						t.Fatalf("Wrong label matchers provided, expected %v, got %v", expectedMatchers, ms)
					}
					return index.ErrPostings(expectedPostingsErr), nil
				})
				t.Cleanup(func() { p.Close() })

				got, err := p.PostingsForMatchers(concurrent, expectedMatchers...)
				if err != nil {
					t.Fatalf("Unexpected err: %s", err)
				}
				if got == nil {
					t.Fatalf("Nil postings returned")
				}
				if got.Err() == nil {
					t.Fatalf("Expected ErrPostings with non nil error, got %T with nil error", got)
				}
				if got.Err().Error() != expectedPostingsErr.Error() {
					t.Fatalf("Expected ErrPostings with err %q, got %T with err %q", expectedPostingsErr, got, got.Err())
				}
			})
		}
	})

	t.Run("err returned", func(t *testing.T) {
		expectedMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
		}
		expectedErr := fmt.Errorf("failed successfully")

		p := newPromisePostingsForMatchersProvider(func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
			return nil, expectedErr
		})
		t.Cleanup(func() { p.Close() })

		_, err := p.PostingsForMatchers(true, expectedMatchers...)
		if err == nil || err.Error() != expectedErr.Error() {
			t.Fatalf("Expected error %q, got %q", expectedErr, err)
		}
	})

	t.Run("happy case multiple concurrent calls: two same one different", func(t *testing.T) {
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
		p := newPromisePostingsForMatchersProvider(func(ifp IndexForPostings, ms ...*labels.Matcher) (index.Postings, error) {
			select {
			case called <- struct{}{}:
			default:
			}
			<-release
			return nil, fmt.Errorf(matchersString(ms))
		})
		t.Cleanup(func() { p.Close() })

		results := make([]string, len(calls))
		resultsWg := sync.WaitGroup{}
		resultsWg.Add(len(calls))

		// make all calls
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
			if results[i] != matchersString(c) {
				t.Errorf("Call %d should have returned error %q, but got %q instead", i, matchersString(c), results[i])
			}
		}
	})
}

type indexForPostingsMock struct{}

func (idx indexForPostingsMock) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	panic("implement me")
}

func (idx indexForPostingsMock) Postings(name string, values ...string) (index.Postings, error) {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = matchersKey(sets[i%matcherSets])
	}
}
