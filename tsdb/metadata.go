package tsdb

import (
	"github.com/grafana/regexp"

	"github.com/prometheus/prometheus/model/labels"
)

var metaLabelRegex = regexp.MustCompile(labels.MetalabelRegex)

func seperateMetaMatchers(ms []*labels.Matcher) ([]*labels.Matcher, []*labels.Matcher) {
	// get the label matchers related to metadata
	metaMatchers := make([]*labels.Matcher, 0)
	normalMatchers := make([]*labels.Matcher, 0)
	for _, m := range ms {
		if metaLabelRegex.MatchString(m.Name) {
			metaMatchers = append(metaMatchers, m)
		} else {
			normalMatchers = append(normalMatchers, m)
		}
	}
	return metaMatchers, normalMatchers
}

func seperateMetaLabels(lbs labels.Labels) (labels.Labels, labels.Labels) {
	// get the label matchers related to metadata
	metaLabels := []labels.Label{}
	normalLabels := []labels.Label{}
	lbs.Range(func(l labels.Label) {
		if metaLabelRegex.MatchString(l.Name) {
			metaLabels = append(metaLabels, l)
		} else {
			normalLabels = append(normalLabels, l)
		}
	})
	return labels.New(metaLabels...), labels.New(normalLabels...)
}
