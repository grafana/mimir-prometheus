package tsdb

const (
	// TODO dimitarvdimitrov establish relative costs here
	costPerIteratedPosting = 1.0
	costPerPostingList     = 10.0
)

type planPredicate struct {
	// selectivity is between 0 and 1. 1 indicates that the matcher will match all label values, 0 indicates it will match no values. NB: label values, not series
	selectivity float64
	// cardinality is the estimation of how many series this matcher matches on its own.
	cardinality         int64
	labelNameUniqueVals float64
	// perMatchCost is how much it costs to run this matcher against an arbitrary label value.
	perMatchCost float64
	// indexScanCost is the perMatchCost to run the matcher against all label values (or at least enough to know all the values it matches).
	indexScanCost float64
}

type plan struct {
	predicates []planPredicate

	applied []bool

	totalSeries int64
}

func (p plan) totalCost() float64 {
	cost := 0.0
	for i, pr := range p.predicates {
		if p.applied[i] {
			cost += p.indexLookupCost(pr)
		}
	}

	cost += p.intersectionCost()

	fetchedSeries := p.intersectionSize()

	for i, m := range p.predicates {
		// In reality we will apply all the predicates for each series and stop once one predicate doesn't match.
		// But we calculate for the worst case where we have to run all predicates for all series.
		if !p.applied[i] {
			cost += p.filterCost(fetchedSeries, m)
		}
	}

	return cost
}

func (p plan) indexLookupCost(pr planPredicate) float64 {
	cost := 0.0
	// Runing the matcher against all label values.
	cost += pr.indexScanCost

	// Retrieving each posting list (e.g. checksumming, disk seeking)
	cost += costPerPostingList * pr.labelNameUniqueVals * pr.selectivity

	return cost
}

func (p plan) intersectionCost() float64 {
	iteratedPostings := int64(0)
	for i, pr := range p.predicates {
		if !p.applied[i] {
			continue
		}

		iteratedPostings += pr.cardinality
	}

	return float64(iteratedPostings) * costPerIteratedPosting
}

func (p plan) intersectionSize() int64 {
	finalSelectivity := 0.0
	for i, pr := range p.predicates {
		if !p.applied[i] {
			continue
		}

		// We use the selectivity across all series instead of the selectivity across label values.
		// For example, if {protocol=~.*} matches all values, it doesn't mean it won't reduce the result set after intersection.
		finalSelectivity *= float64(pr.cardinality) / float64(p.totalSeries)
	}
	return int64(finalSelectivity * float64(p.totalSeries))
}

// filterCost is the perMatchCost to run the matcher against all series.
func (p plan) filterCost(series int64, m planPredicate) float64 {
	return float64(series) * m.perMatchCost
}
