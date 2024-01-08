package tsdb

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

func labelValuesForMatchersStream(ctx context.Context, r IndexReader, name string, matchers []*labels.Matcher) storage.LabelValues {
	// See which labels must be non-empty.
	// Optimization for case like {l=~".", l!="1"}.
	labelMustBeSet := make(map[string]bool, len(matchers))
	for _, m := range matchers {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

	var its, notIts []index.Postings
	for _, m := range matchers {
		switch {
		case labelMustBeSet[m.Name]:
			// If this matcher must be non-empty, we can be smarter.
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp
			switch {
			case isNot && matchesEmpty: // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse, err := m.Inverse()
				if err != nil {
					return storage.ErrLabelValues(err)
				}

				it, err := postingsForMatcher(ctx, r, inverse)
				if err != nil {
					return storage.ErrLabelValues(err)
				}
				notIts = append(notIts, it)
			case isNot && !matchesEmpty: // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse, err := m.Inverse()
				if err != nil {
					return storage.ErrLabelValues(err)
				}

				it, err := inversePostingsForMatcher(ctx, r, inverse)
				if err != nil {
					return storage.ErrLabelValues(err)
				}
				if index.IsEmptyPostingsType(it) {
					return storage.EmptyLabelValues()
				}
				its = append(its, it)
			default: // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it, err := postingsForMatcher(ctx, r, m)
				if err != nil {
					return storage.ErrLabelValues(err)
				}
				if index.IsEmptyPostingsType(it) {
					return storage.EmptyLabelValues()
				}
				its = append(its, it)
			}
		default: // l=""
			// If a matcher for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			it, err := inversePostingsForMatcher(ctx, r, m)
			if err != nil {
				return storage.ErrLabelValues(err)
			}
			notIts = append(notIts, it)
		}
	}

	if len(its) == 0 && len(notIts) > 0 {
		k, v := index.AllPostingsKey()
		allPostings, err := r.Postings(ctx, k, v)
		if err != nil {
			return storage.ErrLabelValues(err)
		}
		its = append(its, allPostings)
	}

	pit := index.Intersect(its...)
	for _, n := range notIts {
		pit = index.Without(pit, n)
	}
	if pit.Err() != nil {
		return storage.ErrLabelValues(pit.Err())
	}

	// Expand up to a certain number of postings, to reduce runtime complexity when filtering label values.
	// If we hit that limit, the rest is unexpanded.
	const expandPostingsLimit = 10_000_000
	var expanded []storage.SeriesRef
	// Go one beyond the limit, so we can tell if the iterator is exhausted
	for len(expanded) <= expandPostingsLimit && pit.Next() {
		expanded = append(expanded, pit.At())
	}
	if pit.Err() != nil {
		return storage.ErrLabelValues(fmt.Errorf("expanding postings for matchers: %w", pit.Err()))
	}
	if len(expanded) > expandPostingsLimit {
		// Couldn't exhaust the iterator
		pit = newPrependPostings(expanded, pit)
	} else {
		pit = index.NewListPostings(expanded)
	}

	return r.LabelValuesFor(pit, name)
}
