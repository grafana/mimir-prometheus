// Copyright 2025 The Prometheus Authors
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
	"go.uber.org/atomic"
)

// NewMatcherWithTimeTracker returns a matcher which can track the time spent running regular expression matchers.
// duration is incremented when the MatchType is MatchRegexp or MatchNotRegexp.
// duration is incremented every power-of-two invocation of Matcher.Matches() (1st, 2nd, 4th, 8th, 16th, ...) and scaled to the sample rate.
func NewMatcherWithTimeTracker(t MatchType, n, v string, duration *atomic.Duration) (*Matcher, error) {
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := NewFastRegexMatcherWithTimeTracker(v, duration)
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

// NewFastRegexMatcherWithTimeTracker returns a matcher which will track the time spent running the matcher.
// duration is incremented every power-of-two invocation of Matcher.Matches() (1st, 2nd, 4th, 8th, 16th, ...) and scaled to the sample rate.
func NewFastRegexMatcherWithTimeTracker(regex string, duration *atomic.Duration) (*FastRegexMatcher, error) {
	m, err := NewFastRegexMatcher(regex)
	if err != nil {
		return nil, err
	}
	withDifferentObserver := *m
	withDifferentObserver.matchesWallClockDuration = duration
	withDifferentObserver.sampler = 0
	return &withDifferentObserver, nil
}
