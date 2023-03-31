// Copyright 2020 The Prometheus Authors
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

package labels

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/regexp"
	"github.com/grafana/regexp/syntax"
	"github.com/stretchr/testify/require"
)

var (
	asciiRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
	regexes    = []string{
		"foo",
		"^foo",
		"(foo|bar)",
		"foo.*",
		".*foo",
		"^.*foo$",
		"^.+foo$",
		".*",
		".+",
		"foo.+",
		".+foo",
		"foo\n.+",
		"foo\n.*",
		".*foo.*",
		".+foo.+",
		"(?s:.*)",
		"(?s:.+)",
		"(?s:^.*foo$)",
		"(?i:foo)",
		"(?i:(foo|bar))",
		"(?i:(foo1|foo2|bar))",
		"^(?i:foo|oo)|(bar)$",
		"(?i:(foo1|foo2|aaa|bbb|ccc|ddd|eee|fff|ggg|hhh|iii|lll|mmm|nnn|ooo|ppp|qqq|rrr|sss|ttt|uuu|vvv|www|xxx|yyy|zzz))",
		"((.*)(bar|b|buzz)(.+)|foo)$",
		"^$",
		"(prometheus|api_prom)_api_v1_.+",
		"10\\.0\\.(1|2)\\.+",
		"10\\.0\\.(1|2).+",
		"((fo(bar))|.+foo)",
		// A long case sensitive alternation.
		"zQPbMkNO|NNSPdvMi|iWuuSoAl|qbvKMimS|IecrXtPa|seTckYqt|NxnyHkgB|fIDlOgKb|UhlWIygH|OtNoJxHG|cUTkFVIV|mTgFIHjr|jQkoIDtE|PPMKxRXl|AwMfwVkQ|CQyMrTQJ|BzrqxVSi|nTpcWuhF|PertdywG|ZZDgCtXN|WWdDPyyE|uVtNQsKk|BdeCHvPZ|wshRnFlH|aOUIitIp|RxZeCdXT|CFZMslCj|AVBZRDxl|IzIGCnhw|ythYuWiz|oztXVXhl|VbLkwqQx|qvaUgyVC|VawUjPWC|ecloYJuj|boCLTdSU|uPrKeAZx|hrMWLWBq|JOnUNHRM|rYnujkPq|dDEdZhIj|DRrfvugG|yEGfDxVV|YMYdJWuP|PHUQZNWM|AmKNrLis|zTxndVfn|FPsHoJnc|EIulZTua|KlAPhdzg|ScHJJCLt|NtTfMzME|eMCwuFdo|SEpJVJbR|cdhXZeCx|sAVtBwRh|kVFEVcMI|jzJrxraA|tGLHTell|NNWoeSaw|DcOKSetX|UXZAJyka|THpMphDP|rizheevl|kDCBRidd|pCZZRqyu|pSygkitl|SwZGkAaW|wILOrfNX|QkwVOerj|kHOMxPDr|EwOVycJv|AJvtzQFS|yEOjKYYB|LizIINLL|JBRSsfcG|YPiUqqNl|IsdEbvee|MjEpGcBm|OxXZVgEQ|xClXGuxa|UzRCGFEb|buJbvfvA|IPZQxRet|oFYShsMc|oBHffuHO|bzzKrcBR|KAjzrGCl|IPUsAVls|OGMUMbIU|gyDccHuR|bjlalnDd|ZLWjeMna|fdsuIlxQ|dVXtiomV|XxedTjNg|XWMHlNoA|nnyqArQX|opfkWGhb|wYtnhdYb",
		// A long case insensitive alternation.
		"(?i:(zQPbMkNO|NNSPdvMi|iWuuSoAl|qbvKMimS|IecrXtPa|seTckYqt|NxnyHkgB|fIDlOgKb|UhlWIygH|OtNoJxHG|cUTkFVIV|mTgFIHjr|jQkoIDtE|PPMKxRXl|AwMfwVkQ|CQyMrTQJ|BzrqxVSi|nTpcWuhF|PertdywG|ZZDgCtXN|WWdDPyyE|uVtNQsKk|BdeCHvPZ|wshRnFlH|aOUIitIp|RxZeCdXT|CFZMslCj|AVBZRDxl|IzIGCnhw|ythYuWiz|oztXVXhl|VbLkwqQx|qvaUgyVC|VawUjPWC|ecloYJuj|boCLTdSU|uPrKeAZx|hrMWLWBq|JOnUNHRM|rYnujkPq|dDEdZhIj|DRrfvugG|yEGfDxVV|YMYdJWuP|PHUQZNWM|AmKNrLis|zTxndVfn|FPsHoJnc|EIulZTua|KlAPhdzg|ScHJJCLt|NtTfMzME|eMCwuFdo|SEpJVJbR|cdhXZeCx|sAVtBwRh|kVFEVcMI|jzJrxraA|tGLHTell|NNWoeSaw|DcOKSetX|UXZAJyka|THpMphDP|rizheevl|kDCBRidd|pCZZRqyu|pSygkitl|SwZGkAaW|wILOrfNX|QkwVOerj|kHOMxPDr|EwOVycJv|AJvtzQFS|yEOjKYYB|LizIINLL|JBRSsfcG|YPiUqqNl|IsdEbvee|MjEpGcBm|OxXZVgEQ|xClXGuxa|UzRCGFEb|buJbvfvA|IPZQxRet|oFYShsMc|oBHffuHO|bzzKrcBR|KAjzrGCl|IPUsAVls|OGMUMbIU|gyDccHuR|bjlalnDd|ZLWjeMna|fdsuIlxQ|dVXtiomV|XxedTjNg|XWMHlNoA|nnyqArQX|opfkWGhb|wYtnhdYb))",
	}
	values = []string{
		"foo", " foo bar", "bar", "buzz\nbar", "bar foo", "bfoo", "\n", "\nfoo", "foo\n", "hello foo world", "hello foo\n world", "",
		"FOO", "Foo", "OO", "Oo", "\nfoo\n", strings.Repeat("f", 20), "prometheus", "prometheus_api_v1", "prometheus_api_v1_foo",
		"10.0.1.20", "10.0.2.10", "10.0.3.30", "10.0.4.40",
		"foofoo0", "foofoo",
	}
)

func TestNewFastRegexMatcher(t *testing.T) {
	for _, r := range regexes {
		r := r
		for _, v := range values {
			v := v
			t.Run(r+` on "`+v+`"`, func(t *testing.T) {
				t.Parallel()
				m, err := NewFastRegexMatcher(r)
				require.NoError(t, err)
				require.Equal(t, m.re.MatchString(v), m.MatchString(v))
			})
		}

	}
}

func BenchmarkNewFastRegexMatcher(b *testing.B) {
	runBenchmark := func(newFunc func(v string) (*FastRegexMatcher, error)) func(b *testing.B) {
		return func(b *testing.B) {
			for _, r := range regexes {
				b.Run(getTestNameFromRegexp(r), func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						_, err := newFunc(r)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}

	b.Run("with cache", runBenchmark(NewFastRegexMatcher))
	b.Run("without cache", runBenchmark(newFastRegexMatcherWithoutCache))
}

func BenchmarkNewFastRegexMatcher_CacheMisses(b *testing.B) {
	// Init the random seed with a constant, so that it doesn't change between runs.
	randGenerator := rand.New(rand.NewSource(1))

	tests := map[string]string{
		"simple regexp":  randString(randGenerator, 10),
		"complex regexp": strings.Join(randStrings(randGenerator, 100, 10), "|"),
	}

	for testName, regexpPrefix := range tests {
		b.Run(testName, func(b *testing.B) {
			// Ensure the cache is empty.
			fastRegexMatcherCache.Purge()

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				// Unique regexp to emulate 100% cache misses.
				regexp := regexpPrefix + strconv.Itoa(n)

				_, err := NewFastRegexMatcher(regexp)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestOptimizeConcatRegex(t *testing.T) {
	cases := []struct {
		regex    string
		prefix   string
		suffix   string
		contains string
	}{
		{regex: "foo(hello|bar)", prefix: "foo", suffix: "", contains: ""},
		{regex: "foo(hello|bar)world", prefix: "foo", suffix: "world", contains: ""},
		{regex: "foo.*", prefix: "foo", suffix: "", contains: ""},
		{regex: "foo.*hello.*bar", prefix: "foo", suffix: "bar", contains: "hello"},
		{regex: ".*foo", prefix: "", suffix: "foo", contains: ""},
		{regex: "^.*foo$", prefix: "", suffix: "foo", contains: ""},
		{regex: ".*foo.*", prefix: "", suffix: "", contains: "foo"},
		{regex: ".*foo.*bar.*", prefix: "", suffix: "", contains: "foo"},
		{regex: ".*(foo|bar).*", prefix: "", suffix: "", contains: ""},
		{regex: ".*[abc].*", prefix: "", suffix: "", contains: ""},
		{regex: ".*((?i)abc).*", prefix: "", suffix: "", contains: ""},
		{regex: ".*(?i:abc).*", prefix: "", suffix: "", contains: ""},
		{regex: "(?i:abc).*", prefix: "", suffix: "", contains: ""},
		{regex: ".*(?i:abc)", prefix: "", suffix: "", contains: ""},
		{regex: ".*(?i:abc)def.*", prefix: "", suffix: "", contains: "def"},
		{regex: "(?i).*(?-i:abc)def", prefix: "", suffix: "", contains: "abc"},
		{regex: ".*(?msU:abc).*", prefix: "", suffix: "", contains: "abc"},
		{regex: "[aA]bc.*", prefix: "", suffix: "", contains: "bc"},
		{regex: "^5..$", prefix: "5", suffix: "", contains: ""},
		{regex: "^release.*", prefix: "release", suffix: "", contains: ""},
		{regex: "^env-[0-9]+laio[1]?[^0-9].*", prefix: "env-", suffix: "", contains: "laio"},
	}

	for _, c := range cases {
		parsed, err := syntax.Parse(c.regex, syntax.Perl)
		require.NoError(t, err)

		prefix, suffix, contains := optimizeConcatRegex(parsed)
		require.Equal(t, c.prefix, prefix)
		require.Equal(t, c.suffix, suffix)
		require.Equal(t, c.contains, contains)
	}
}

// Refer to https://github.com/prometheus/prometheus/issues/2651.
func TestFindSetMatches(t *testing.T) {
	for _, c := range []struct {
		pattern          string
		expMatches       []string
		expCaseSensitive bool
	}{
		// Single value, coming from a `bar=~"foo"` selector.
		{"foo", []string{"foo"}, true},
		{"^foo", []string{"foo"}, true},
		{"^foo$", []string{"foo"}, true},
		// Simple sets alternates.
		{"foo|bar|zz", []string{"foo", "bar", "zz"}, true},
		// Simple sets alternate and concat (bar|baz is parsed as "ba[rz]").
		{"foo|bar|baz", []string{"foo", "bar", "baz"}, true},
		// Simple sets alternate and concat and capture
		{"foo|bar|baz|(zz)", []string{"foo", "bar", "baz", "zz"}, true},
		// Simple sets alternate and concat and alternates with empty matches
		// parsed as  b(ar|(?:)|uzz) where b(?:) means literal b.
		{"bar|b|buzz", []string{"bar", "b", "buzz"}, true},
		// Skip outer anchors (it's enforced anyway at the root).
		{"^(bar|b|buzz)$", []string{"bar", "b", "buzz"}, true},
		{"^(?:prod|production)$", []string{"prod", "production"}, true},
		// Do not optimize regexp with inner anchors.
		{"(bar|b|b^uz$z)", nil, false},
		// Do not optimize regexp with empty string matcher.
		{"^$|Running", nil, false},
		// Simple sets containing escaped characters.
		{"fo\\.o|bar\\?|\\^baz", []string{"fo.o", "bar?", "^baz"}, true},
		// using charclass
		{"[abc]d", []string{"ad", "bd", "cd"}, true},
		// high low charset different => A(B[CD]|EF)|BC[XY]
		{"ABC|ABD|AEF|BCX|BCY", []string{"ABC", "ABD", "AEF", "BCX", "BCY"}, true},
		// triple concat
		{"api_(v1|prom)_push", []string{"api_v1_push", "api_prom_push"}, true},
		// triple concat with multiple alternates
		{"(api|rpc)_(v1|prom)_push", []string{"api_v1_push", "api_prom_push", "rpc_v1_push", "rpc_prom_push"}, true},
		{"(api|rpc)_(v1|prom)_(push|query)", []string{"api_v1_push", "api_v1_query", "api_prom_push", "api_prom_query", "rpc_v1_push", "rpc_v1_query", "rpc_prom_push", "rpc_prom_query"}, true},
		// class starting with "-"
		{"[-1-2][a-c]", []string{"-a", "-b", "-c", "1a", "1b", "1c", "2a", "2b", "2c"}, true},
		{"[1^3]", []string{"1", "3", "^"}, true},
		// OpPlus with concat
		{"(.+)/(foo|bar)", nil, false},
		// Simple sets containing special characters without escaping.
		{"fo.o|bar?|^baz", nil, false},
		// case sensitive wrapper.
		{"(?i)foo", []string{"FOO"}, false},
		// case sensitive wrapper on alternate.
		{"(?i)foo|bar|baz", []string{"FOO", "BAR", "BAZ", "BAr", "BAz"}, false},
		// mixed case sensitivity.
		{"(api|rpc)_(v1|prom)_((?i)push|query)", nil, false},
		// mixed case sensitivity concatenation only without capture group.
		{"api_v1_(?i)push", nil, false},
		// mixed case sensitivity alternation only without capture group.
		{"api|(?i)rpc", nil, false},
		// case sensitive after unsetting insensitivity.
		{"rpc|(?i)(?-i)api", []string{"rpc", "api"}, true},
		// case sensitive after unsetting insensitivity in all alternation options.
		{"(?i)((?-i)api|(?-i)rpc)", []string{"api", "rpc"}, true},
		// mixed case sensitivity after unsetting insensitivity.
		{"(?i)rpc|(?-i)api", nil, false},
		// too high charset combination
		{"(api|rpc)_[^0-9]", nil, false},
		// too many combinations
		{"[a-z][a-z]", nil, false},
	} {
		c := c
		t.Run(c.pattern, func(t *testing.T) {
			t.Parallel()
			parsed, err := syntax.Parse(c.pattern, syntax.Perl)
			require.NoError(t, err)
			matches, actualCaseSensitive := findSetMatches(parsed)
			require.Equal(t, c.expMatches, matches)
			require.Equal(t, c.expCaseSensitive, actualCaseSensitive)
		})
	}
}

func BenchmarkFastRegexMatcher(b *testing.B) {
	// Init the random seed with a constant, so that it doesn't change between runs.
	randGenerator := rand.New(rand.NewSource(1))

	// Generate variable lengths random texts to match against.
	texts := append([]string{}, randStrings(randGenerator, 10, 10)...)
	texts = append(texts, randStrings(randGenerator, 5, 30)...)
	texts = append(texts, randStrings(randGenerator, 1, 100)...)
	texts = append(texts, "foo"+randString(randGenerator, 50))
	texts = append(texts, randString(randGenerator, 50)+"foo")

	for _, r := range regexes {
		b.Run(getTestNameFromRegexp(r), func(b *testing.B) {
			m, err := NewFastRegexMatcher(r)
			require.NoError(b, err)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, text := range texts {
					_ = m.MatchString(text)
				}
			}
		})
	}
}

func Test_OptimizeRegex(t *testing.T) {
	for _, c := range []struct {
		pattern string
		exp     StringMatcher
	}{
		{".*", anyStringWithoutNewlineMatcher{}},
		{".*?", anyStringWithoutNewlineMatcher{}},
		{"(?s:.*)", trueMatcher{}},
		{"(.*)", anyStringWithoutNewlineMatcher{}},
		{"^.*$", anyStringWithoutNewlineMatcher{}},
		{".+", &anyNonEmptyStringMatcher{matchNL: false}},
		{"(?s:.+)", &anyNonEmptyStringMatcher{matchNL: true}},
		{"^.+$", &anyNonEmptyStringMatcher{matchNL: false}},
		{"(.+)", &anyNonEmptyStringMatcher{matchNL: false}},
		{"", emptyStringMatcher{}},
		{"^$", emptyStringMatcher{}},
		{"^foo$", &equalStringMatcher{s: "foo", caseSensitive: true}},
		{"^(?i:foo)$", &equalStringMatcher{s: "FOO", caseSensitive: false}},
		{"^((?i:foo)|(bar))$", orStringMatcher([]StringMatcher{&equalStringMatcher{s: "FOO", caseSensitive: false}, &equalStringMatcher{s: "bar", caseSensitive: true}})},
		{"^((?i:foo|oo)|(bar))$", orStringMatcher([]StringMatcher{&equalStringMatcher{s: "FOO", caseSensitive: false}, &equalStringMatcher{s: "OO", caseSensitive: false}, &equalStringMatcher{s: "bar", caseSensitive: true}})},
		{"(?i:(foo1|foo2|bar))", orStringMatcher([]StringMatcher{orStringMatcher([]StringMatcher{&equalStringMatcher{s: "FOO1", caseSensitive: false}, &equalStringMatcher{s: "FOO2", caseSensitive: false}}), &equalStringMatcher{s: "BAR", caseSensitive: false}})},
		{".*foo.*", &containsStringMatcher{substrings: []string{"foo"}, left: anyStringWithoutNewlineMatcher{}, right: anyStringWithoutNewlineMatcher{}}},
		{"(.*)foo.*", &containsStringMatcher{substrings: []string{"foo"}, left: anyStringWithoutNewlineMatcher{}, right: anyStringWithoutNewlineMatcher{}}},
		{"(.*)foo(.*)", &containsStringMatcher{substrings: []string{"foo"}, left: anyStringWithoutNewlineMatcher{}, right: anyStringWithoutNewlineMatcher{}}},
		{"(.+)foo(.*)", &containsStringMatcher{substrings: []string{"foo"}, left: &anyNonEmptyStringMatcher{matchNL: false}, right: anyStringWithoutNewlineMatcher{}}},
		{"^.+foo.+", &containsStringMatcher{substrings: []string{"foo"}, left: &anyNonEmptyStringMatcher{matchNL: false}, right: &anyNonEmptyStringMatcher{matchNL: false}}},
		{"^(.*)(foo)(.*)$", &containsStringMatcher{substrings: []string{"foo"}, left: anyStringWithoutNewlineMatcher{}, right: anyStringWithoutNewlineMatcher{}}},
		{"^(.*)(foo|foobar)(.*)$", &containsStringMatcher{substrings: []string{"foo", "foobar"}, left: anyStringWithoutNewlineMatcher{}, right: anyStringWithoutNewlineMatcher{}}},
		{"^(.*)(foo|foobar)(.+)$", &containsStringMatcher{substrings: []string{"foo", "foobar"}, left: anyStringWithoutNewlineMatcher{}, right: &anyNonEmptyStringMatcher{matchNL: false}}},
		{"^(.*)(bar|b|buzz)(.+)$", &containsStringMatcher{substrings: []string{"bar", "b", "buzz"}, left: anyStringWithoutNewlineMatcher{}, right: &anyNonEmptyStringMatcher{matchNL: false}}},
		{"10\\.0\\.(1|2)\\.+", nil},
		{"10\\.0\\.(1|2).+", &containsStringMatcher{substrings: []string{"10.0.1", "10.0.2"}, left: nil, right: &anyNonEmptyStringMatcher{matchNL: false}}},
		{"^.+foo", &containsStringMatcher{substrings: []string{"foo"}, left: &anyNonEmptyStringMatcher{matchNL: false}, right: nil}},
		{"foo-.*$", &containsStringMatcher{substrings: []string{"foo-"}, left: nil, right: anyStringWithoutNewlineMatcher{}}},
		{"(prometheus|api_prom)_api_v1_.+", &containsStringMatcher{substrings: []string{"prometheus_api_v1_", "api_prom_api_v1_"}, left: nil, right: &anyNonEmptyStringMatcher{matchNL: false}}},
		{"^((.*)(bar|b|buzz)(.+)|foo)$", orStringMatcher([]StringMatcher{&containsStringMatcher{substrings: []string{"bar", "b", "buzz"}, left: anyStringWithoutNewlineMatcher{}, right: &anyNonEmptyStringMatcher{matchNL: false}}, &equalStringMatcher{s: "foo", caseSensitive: true}})},
		{"((fo(bar))|.+foo)", orStringMatcher([]StringMatcher{orStringMatcher([]StringMatcher{&equalStringMatcher{s: "fobar", caseSensitive: true}}), &containsStringMatcher{substrings: []string{"foo"}, left: &anyNonEmptyStringMatcher{matchNL: false}, right: nil}})},
		{"(.+)/(gateway|cortex-gw|cortex-gw-internal)", &containsStringMatcher{substrings: []string{"/gateway", "/cortex-gw", "/cortex-gw-internal"}, left: &anyNonEmptyStringMatcher{matchNL: false}, right: nil}},
		// we don't support case insensitive matching for contains.
		// This is because there's no strings.IndexOfFold function.
		// We can revisit later if this is really popular by using strings.ToUpper.
		{"^(.*)((?i)foo|foobar)(.*)$", nil},
		{"(api|rpc)_(v1|prom)_((?i)push|query)", nil},
		{"[a-z][a-z]", nil},
		{"[1^3]", nil},
		{".*foo.*bar.*", nil},
		{`\d*`, nil},
		{".", nil},
		// This one is not supported because  `stringMatcherFromRegexp` is not reentrant for syntax.OpConcat.
		// It would make the code too complex to handle it.
		{"/|/bar.*", nil},
		{"(.+)/(foo.*|bar$)", nil},
	} {
		c := c
		t.Run(c.pattern, func(t *testing.T) {
			t.Parallel()
			parsed, err := syntax.Parse(c.pattern, syntax.Perl)
			require.NoError(t, err)
			matches := stringMatcherFromRegexp(parsed)
			require.Equal(t, c.exp, matches)
		})
	}
}

func randString(randGenerator *rand.Rand, length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = asciiRunes[randGenerator.Intn(len(asciiRunes))]
	}
	return string(b)
}

func randStrings(randGenerator *rand.Rand, many, length int) []string {
	out := make([]string, 0, many)
	for i := 0; i < many; i++ {
		out = append(out, randString(randGenerator, length))
	}
	return out
}

func FuzzFastRegexMatcher_WithStaticallyDefinedRegularExpressions(f *testing.F) {
	// Create all matchers.
	matchers := make([]*FastRegexMatcher, 0, len(regexes))
	for _, re := range regexes {
		m, err := NewFastRegexMatcher(re)
		require.NoError(f, err)
		matchers = append(matchers, m)
	}

	// Add known values to seed corpus.
	for _, v := range values {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, text string) {
		for _, m := range matchers {
			require.Equalf(t, m.re.MatchString(text), m.MatchString(text), "regexp: %s text: %s", m.re.String(), text)
		}
	})
}

func FuzzFastRegexMatcher_WithFuzzyRegularExpressions(f *testing.F) {
	for _, re := range regexes {
		for _, text := range values {
			f.Add(re, text)
		}
	}

	f.Fuzz(func(t *testing.T, re, text string) {
		m, err := NewFastRegexMatcher(re)
		if err != nil {
			// Ignore invalid regexes.
			return
		}

		require.Equalf(t, m.re.MatchString(text), m.MatchString(text), "regexp: %s text: %s", m.re.String(), text)
	})
}

// This test can be used to analyze real queries from Mimir logs. You can extract real queries with a regexp matcher
// running the following command:
//
// logcli --addr=XXX --username=YYY --password=ZZZ query '{namespace=~"(cortex|mimir).*",name="query-frontend"} |= "query stats" |= "=~" --limit=100000 > logs.txt
func TestAnalyzeRealQueries(t *testing.T) {
	t.Skip("Decomment this test only to manually analyze real queries")

	type labelValueInfo struct {
		numMatchingQueries       int     //nolint:unused
		numShardedQueries        int     //nolint:unused
		numSplitQueries          int     //nolint:unused
		optimized                bool    //nolint:unused
		averageParsingTimeMillis float64 //nolint:unused

		// Sorted list of timestamps when the queries have been received.
		queryStartTimes []time.Time
	}

	labelValueRE := regexp.MustCompile(`=~\\"([^"]+)\\"`)
	tsRE := regexp.MustCompile(`ts=([^ ]+)`)
	shardedQueriesRE := regexp.MustCompile(`sharded_queries=(\d+)`)
	splitQueriesRE := regexp.MustCompile(`split_queries=(\d+)`)

	labelValues := make(map[string]*labelValueInfo)

	// Read the logs file line-by-line, and find all values for regex label matchers.
	readFile, err := os.Open("logs.txt")
	require.NoError(t, err)

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	numQueries := 0

	for fileScanner.Scan() {
		line := fileScanner.Text()
		matches := labelValueRE.FindAllStringSubmatch(line, -1)
		if len(matches) == 0 {
			continue
		}

		// Look up query stats.
		tsRaw := tsRE.FindStringSubmatch(line)
		shardedQueriesRaw := shardedQueriesRE.FindStringSubmatch(line)
		splitQueriesRaw := splitQueriesRE.FindStringSubmatch(line)
		shardedQueries := 0
		splitQueries := 0
		var ts time.Time

		if len(tsRaw) > 0 {
			ts, _ = time.Parse(time.RFC3339Nano, tsRaw[1])
		}
		if len(shardedQueriesRaw) > 0 {
			shardedQueries, _ = strconv.Atoi(shardedQueriesRaw[1])
		}
		if len(splitQueriesRaw) > 0 {
			splitQueries, _ = strconv.Atoi(splitQueriesRaw[1])
		}

		numQueries++

		for _, match := range matches {
			info := labelValues[match[1]]
			if info == nil {
				info = &labelValueInfo{}
				labelValues[match[1]] = info
			}

			info.numMatchingQueries++
			info.numShardedQueries += shardedQueries
			info.numSplitQueries += splitQueries

			if !ts.IsZero() {
				info.queryStartTimes = append(info.queryStartTimes, ts)
			}
		}
	}

	// Sort query start times.
	for _, info := range labelValues {
		sort.Slice(info.queryStartTimes, func(i, j int) bool {
			return info.queryStartTimes[i].Before(info.queryStartTimes[j])
		})
	}

	require.NoError(t, readFile.Close())
	t.Logf("Found %d unique regexp matchers out of %d queries", len(labelValues), numQueries)

	// Analyze each regexp matcher found.
	numChecked := 0
	numOptimized := 0

	for re, info := range labelValues {
		m, err := NewFastRegexMatcher(re)
		if err != nil {
			// Ignore it, because we may have failed to extract the label matcher.
			continue
		}

		numChecked++

		// Check if each regexp matcher is supported by our optimization.
		if m.isOptimized() {
			numOptimized++
			info.optimized = true
		}

		// Estimate the parsing complexity.
		startTime := time.Now()
		const numParsingRuns = 1000

		for i := 0; i < numParsingRuns; i++ {
			NewFastRegexMatcher(re)
		}

		info.averageParsingTimeMillis = float64(time.Since(startTime).Milliseconds()) / float64(numParsingRuns)
	}

	t.Logf("Found %d out of %d (%.2f%%) regexp matchers optimized by FastRegexMatcher", numOptimized, numChecked, (float64(numOptimized)/float64(numChecked))*100)

	// Print some statistics.
	for labelValue, info := range labelValues {
		// Find the min/avg/max difference between query start times.
		var (
			minQueryStartTimeDiff time.Duration
			maxQueryStartTimeDiff time.Duration
			avgQueryStartTimeDiff time.Duration
			sumQueryStartTime     time.Duration
			countQueryStartTime   int
		)

		for i := 1; i < len(info.queryStartTimes); i++ {
			diff := info.queryStartTimes[i].Sub(info.queryStartTimes[i-1])

			sumQueryStartTime += diff
			countQueryStartTime++

			if minQueryStartTimeDiff == 0 || diff < minQueryStartTimeDiff {
				minQueryStartTimeDiff = diff
			}
			if diff > maxQueryStartTimeDiff {
				maxQueryStartTimeDiff = diff
			}
		}

		if countQueryStartTime > 0 {
			avgQueryStartTimeDiff = sumQueryStartTime / time.Duration(countQueryStartTime)
		}

		t.Logf("num queries: %d\t num split queries: %d\t num sharded queries: %d\t optimized: %t\t parsing time: %.0fms\t min/avg/max query start time diff (sec): %.2f/%.2f/%.2f regexp: %s",
			info.numMatchingQueries, info.numSplitQueries, info.numShardedQueries, info.optimized, info.averageParsingTimeMillis,
			minQueryStartTimeDiff.Seconds(), avgQueryStartTimeDiff.Seconds(), maxQueryStartTimeDiff.Seconds(), labelValue)
	}
}

func TestOptimizeEqualStringMatchers(t *testing.T) {
	tests := map[string]struct {
		input                 StringMatcher
		expectedValues        map[string]struct{}
		expectedCaseSensitive bool
	}{
		"should skip optimization on orStringMatcher with containsStringMatcher": {
			input: orStringMatcher{
				&equalStringMatcher{s: "FOO", caseSensitive: true},
				&containsStringMatcher{substrings: []string{"a", "b", "c"}},
			},
			expectedValues: nil,
		},
		"should run optimization on orStringMatcher with equalStringMatcher and same case sensitivity": {
			input: orStringMatcher{
				&equalStringMatcher{s: "FOO", caseSensitive: true},
				&equalStringMatcher{s: "bar", caseSensitive: true},
				&equalStringMatcher{s: "baz", caseSensitive: true},
			},
			expectedValues: map[string]struct{}{
				"FOO": {},
				"bar": {},
				"baz": {},
			},
			expectedCaseSensitive: true,
		},
		"should skip optimization on orStringMatcher with equalStringMatcher but different case sensitivity": {
			input: orStringMatcher{
				&equalStringMatcher{s: "FOO", caseSensitive: true},
				&equalStringMatcher{s: "bar", caseSensitive: false},
				&equalStringMatcher{s: "baz", caseSensitive: true},
			},
			expectedValues: nil,
		},
		"should run optimization on orStringMatcher with nested orStringMatcher and equalStringMatcher, and same case sensitivity": {
			input: orStringMatcher{
				&equalStringMatcher{s: "FOO", caseSensitive: true},
				orStringMatcher{
					&equalStringMatcher{s: "bar", caseSensitive: true},
					&equalStringMatcher{s: "xxx", caseSensitive: true},
				},
				&equalStringMatcher{s: "baz", caseSensitive: true},
			},
			expectedValues: map[string]struct{}{
				"FOO": {},
				"bar": {},
				"xxx": {},
				"baz": {},
			},
			expectedCaseSensitive: true,
		},
		"should skip optimization on orStringMatcher with nested orStringMatcher and equalStringMatcher, but different case sensitivity": {
			input: orStringMatcher{
				&equalStringMatcher{s: "FOO", caseSensitive: true},
				orStringMatcher{
					// Case sensitivity is different within items at the same level.
					&equalStringMatcher{s: "bar", caseSensitive: true},
					&equalStringMatcher{s: "xxx", caseSensitive: false},
				},
				&equalStringMatcher{s: "baz", caseSensitive: true},
			},
			expectedValues: nil,
		},
		"should skip optimization on orStringMatcher with nested orStringMatcher and equalStringMatcher, but different case sensitivity in the nested one": {
			input: orStringMatcher{
				&equalStringMatcher{s: "FOO", caseSensitive: true},
				// Case sensitivity is different between the parent and child.
				orStringMatcher{
					&equalStringMatcher{s: "bar", caseSensitive: false},
					&equalStringMatcher{s: "xxx", caseSensitive: false},
				},
				&equalStringMatcher{s: "baz", caseSensitive: true},
			},
			expectedValues: nil,
		},
		"should return lowercase values on case insensitive matchers": {
			input: orStringMatcher{
				&equalStringMatcher{s: "FOO", caseSensitive: false},
				orStringMatcher{
					&equalStringMatcher{s: "bAr", caseSensitive: false},
				},
				&equalStringMatcher{s: "baZ", caseSensitive: false},
			},
			expectedValues: map[string]struct{}{
				"foo": {},
				"bar": {},
				"baz": {},
			},
			expectedCaseSensitive: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualMatcher := optimizeEqualStringMatchers(testData.input, 0)

			if testData.expectedValues == nil {
				require.IsType(t, testData.input, actualMatcher)
			} else {
				require.IsType(t, &equalMultiStringMatcher{}, actualMatcher)
				require.Equal(t, testData.expectedValues, actualMatcher.(*equalMultiStringMatcher).values)
				require.Equal(t, testData.expectedCaseSensitive, actualMatcher.(*equalMultiStringMatcher).caseSensitive)
			}
		})
	}
}

// This benchmark is used to find a good threshold to use to apply the optimization
// done by optimizeEqualStringMatchers()
func BenchmarkOptimizeEqualStringMatchers(b *testing.B) {
	randGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate variable lengths random texts to match against.
	texts := append([]string{}, randStrings(randGenerator, 10, 10)...)
	texts = append(texts, randStrings(randGenerator, 5, 30)...)
	texts = append(texts, randStrings(randGenerator, 1, 100)...)

	for numAlternations := 2; numAlternations <= 256; numAlternations *= 2 {
		for _, caseSensitive := range []bool{true, false} {
			b.Run(fmt.Sprintf("alternations: %d case sensitive: %t", numAlternations, caseSensitive), func(b *testing.B) {
				// Generate a regex with the expected number of alternations.
				re := strings.Join(randStrings(randGenerator, numAlternations, 10), "|")
				if !caseSensitive {
					re = "(?i:(" + re + "))"
				}

				parsed, err := syntax.Parse(re, syntax.Perl)
				require.NoError(b, err)

				unoptimized := stringMatcherFromRegexpInternal(parsed)
				require.IsType(b, orStringMatcher{}, unoptimized)

				optimized := optimizeEqualStringMatchers(unoptimized, 0)
				require.IsType(b, &equalMultiStringMatcher{}, optimized)

				b.Run("without optimizeEqualStringMatchers()", func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						for _, t := range texts {
							unoptimized.Matches(t)
						}
					}
				})

				b.Run("with optimizeEqualStringMatchers()", func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						for _, t := range texts {
							optimized.Matches(t)
						}
					}
				})
			})
		}
	}
}

func getTestNameFromRegexp(re string) string {
	if len(re) > 32 {
		return re[:32]
	}
	return re
}
