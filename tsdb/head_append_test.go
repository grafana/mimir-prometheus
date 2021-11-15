package tsdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddJitterToChunkEndTime_Jitter(t *testing.T) {
	chunkMinTime := int64(10)
	nextAt := int64(95)
	maxNextAt := int64(100)
	variance := 0.2

	for i := 0; i < 1000; i++ {
		actual := addJitterToChunkEndTime(chunkMinTime, nextAt, maxNextAt, variance)
		require.GreaterOrEqual(t, actual, int64(95-8))
		require.LessOrEqual(t, actual, maxNextAt)
	}
}

func TestAddJitterToChunkEndTime_ShouldNotApplyJitterToTheLastChunkOfTheRange(t *testing.T) {
	// Since the jitter could also be 0, we try it multiple times.
	for i := 0; i < 10; i++ {
		require.Equal(t, int64(200), addJitterToChunkEndTime(150, 200, 200, 0.2))
	}
}

func TestAddJitterToChunkEndTime_ShouldNotApplyJitterIfDisabled(t *testing.T) {
	// Since the jitter could also be 0, we try it multiple times.
	for i := 0; i < 10; i++ {
		require.Equal(t, int64(130), addJitterToChunkEndTime(100, 130, 200, 0))
	}
}
