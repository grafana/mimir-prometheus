package tsdb

import (
	"context"
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
)

func TestAddJitterToChunkEndTime_ShouldHonorMaxVarianceAndMaxNextAt(t *testing.T) {
	chunkMinTime := int64(10)
	nextAt := int64(95)
	maxNextAt := int64(100)
	variance := 0.2

	// Compute the expected max variance.
	expectedMaxVariance := int64(float64(nextAt-chunkMinTime) * variance)

	for seriesHash := uint64(0); seriesHash < 1000; seriesHash++ {
		actual := addJitterToChunkEndTime(seriesHash, chunkMinTime, nextAt, maxNextAt, variance)
		require.GreaterOrEqual(t, actual, nextAt-(expectedMaxVariance/2))
		require.LessOrEqual(t, actual, maxNextAt)
	}
}

func TestAddJitterToChunkEndTime_Distribution(t *testing.T) {
	chunkMinTime := int64(0)
	nextAt := int64(50)
	maxNextAt := int64(100)
	variance := 0.2
	numSeries := uint64(1000)

	// Compute the expected max variance.
	expectedMaxVariance := int64(float64(nextAt-chunkMinTime) * variance)

	// Keep track of the distribution of the applied variance.
	varianceDistribution := map[int64]int64{}

	for seriesHash := uint64(0); seriesHash < numSeries; seriesHash++ {
		actual := addJitterToChunkEndTime(seriesHash, chunkMinTime, nextAt, maxNextAt, variance)
		require.GreaterOrEqual(t, actual, nextAt-(expectedMaxVariance/2))
		require.LessOrEqual(t, actual, nextAt+(expectedMaxVariance/2))
		require.LessOrEqual(t, actual, maxNextAt)

		variance := nextAt - actual
		varianceDistribution[variance]++
	}

	// Ensure a uniform distribution.
	for variance, count := range varianceDistribution {
		require.Equalf(t, int64(numSeries)/expectedMaxVariance, count, "variance = %d", variance)
	}
}

func TestAddJitterToChunkEndTime_ShouldNotApplyJitterToTheLastChunkOfTheRange(t *testing.T) {
	// Since the jitter could also be 0, we try it for multiple series.
	for seriesHash := uint64(0); seriesHash < 10; seriesHash++ {
		require.Equal(t, int64(200), addJitterToChunkEndTime(seriesHash, 150, 200, 200, 0.2))
	}
}

func TestAddJitterToChunkEndTime_ShouldNotApplyJitterIfDisabled(t *testing.T) {
	// Since the jitter could also be 0, we try it for multiple series.
	for seriesHash := uint64(0); seriesHash < 10; seriesHash++ {
		require.Equal(t, int64(130), addJitterToChunkEndTime(seriesHash, 100, 130, 200, 0))
	}
}

func BenchmarkAppenderAppend_NewSeries(b *testing.B) {
	labelCounts := []int{2, 5, 11, 23, 61, 97} // ~15M series

	b.Run("single threaded", func(b *testing.B) {
		require.Less(b, b.N, multiply(labelCounts))

		db := openTestDB(b, DefaultOptions(), nil)
		b.Cleanup(func() { require.NoError(b, db.Close()) })

		app := db.Appender(context.Background())
		b.ResetTimer()
		b.Cleanup(func() { require.NoError(b, app.Commit()) })

		for i := 0; i < b.N; i++ {
			lset := buildLabelSet(labelCounts, i)
			_, err := app.Append(0, lset, 1_000, 0)
			require.NoError(b, err)
		}
	})

	b.Run("concurrent", func(b *testing.B) {
		db := openTestDB(b, DefaultOptions(), nil)
		b.Cleanup(func() { require.NoError(b, db.Close()) })
		id := &atomic.Int64{}

		b.RunParallel(func(pb *testing.PB) {
			app := db.Appender(context.Background())
			b.Cleanup(func() { require.NoError(b, app.Commit()) })

			for pb.Next() {
				lset := buildLabelSet(labelCounts, int(id.Inc()))
				_, err := app.Append(0, lset, 1_000, 0)
				require.NoError(b, err)
			}
		})
	})
}

func BenchmarkBuildLabelSet(b *testing.B) {
	labelCounts := []int{2, 5, 11, 23, 61, 97} // ~15M series
	require.Less(b, b.N, multiply(labelCounts))

	for i := 0; i < b.N; i++ {
		_ = buildLabelSet(labelCounts, i)
	}
}

func TestBuildLabelSet(t *testing.T) {
	lset := buildLabelSet([]int{2, 5}, 3)
	require.Equal(t, labels.FromStrings("label_0", "label_0    value_1", "label_1", "label_1    value_3"), lset)
}

func buildLabelSet(labelCounts []int, id int) labels.Labels {
	ls := make([]string, 0, len(labelCounts)*2)
	for idx, card := range labelCounts {
		template := []byte("label_     value_      ")
		name := strconv.AppendInt(template[:6], int64(idx), 10)
		value := strconv.AppendInt(template[:17], int64(id%card), 10)
		ls = append(ls, *(*string)(unsafe.Pointer(&name)), *(*string)(unsafe.Pointer(&value)))
	}

	return labels.FromStrings(ls...)
}

func multiply(ints []int) int {
	x := 1
	for _, val := range ints {
		x *= val
	}
	return x
}
