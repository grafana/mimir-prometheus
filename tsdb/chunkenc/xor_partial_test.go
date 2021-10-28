package chunkenc

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXORPartialChunk_Iterator(t *testing.T) {
	for _, extraAppend := range []int{0, 3} {
		t.Run(fmt.Sprintf("extra append = %v", extraAppend), func(t *testing.T) {
			expectedNumSamples := 300

			// Append samples to XORChunk and keep track of all samples appended.
			orig, appended := generateXORChunk(t, expectedNumSamples)

			// Create the partial chunk.
			partial := NewXORPartialChunkFromXORChunk(orig, getLastSamples(appended))

			if extraAppend > 0 {
				// Append more samples to the original one.
				appender, err := orig.Appender()
				require.NoError(t, err)

				for i := 0; i < extraAppend; i++ {
					appender.Append(int64(i), float64(i))
				}
			}

			// Check the number of samples.
			require.Equal(t, expectedNumSamples, partial.NumSamples())
			require.Equal(t, expectedNumSamples+extraAppend, orig.NumSamples())

			// Iterate throw both chunks and expect same samples.
			origIt := orig.Iterator(nil)
			partialIt := partial.Iterator(nil)

			for i := 0; i < expectedNumSamples; i++ {
				require.True(t, origIt.Next())
				require.True(t, partialIt.Next())

				origTs, origValue := origIt.At()
				partialTs, partialValue := partialIt.At()
				assert.Equal(t, origTs, partialTs)
				assert.Equal(t, origValue, partialValue)
			}

			require.NoError(t, origIt.Err())
			require.NoError(t, partialIt.Err())
		})
	}
}

func TestXORPartialChunk_Serialization(t *testing.T) {
	for _, extraAppend := range []int{0, 3} {
		t.Run(fmt.Sprintf("extra append = %v", extraAppend), func(t *testing.T) {
			expectedNumSamples := 300

			// Append samples to XORChunk and keep track of all samples appended.
			orig, appended := generateXORChunk(t, expectedNumSamples)

			// Create the partial chunk.
			partial := NewXORPartialChunkFromXORChunk(orig, getLastSamples(appended))

			if extraAppend > 0 {
				// Append more samples to the original one.
				appender, err := orig.Appender()
				require.NoError(t, err)

				for i := 0; i < extraAppend; i++ {
					appender.Append(int64(i), float64(i))
				}
			}

			// Serialize and deserialize the partial one.
			var err error
			partial, err = NewXORPartialChunkFromWire(partial.Bytes())
			require.NoError(t, err)

			// Check the number of samples.
			require.Equal(t, expectedNumSamples, partial.NumSamples())
			require.Equal(t, expectedNumSamples+extraAppend, orig.NumSamples())

			// Iterate throw both chunks and expect same samples.
			origIt := orig.Iterator(nil)
			partialIt := partial.Iterator(nil)

			for i := 0; i < expectedNumSamples; i++ {
				require.True(t, origIt.Next())
				require.True(t, partialIt.Next())

				origTs, origValue := origIt.At()
				partialTs, partialValue := partialIt.At()
				assert.Equal(t, origTs, partialTs)
				assert.Equal(t, origValue, partialValue)
			}

			require.NoError(t, origIt.Err())
			require.NoError(t, partialIt.Err())
		})
	}
}

func generateXORChunk(t *testing.T, numSamples int) (*XORChunk, []Sample) {
	orig := NewXORChunk()

	app, err := orig.Appender()
	require.NoError(t, err)

	var (
		ts       = int64(1234123324)
		v        = 1243535.123
		appended []Sample
	)

	for i := 0; i < numSamples; i++ {
		ts += int64(rand.Intn(10000) + 1)
		if i%2 == 0 {
			v += float64(rand.Intn(1000000))
		} else {
			v -= float64(rand.Intn(1000000))
		}

		app.Append(ts, v)
		appended = append(appended, Sample{T: ts, V: v})
	}

	return orig, appended
}

func getLastSamples(input []Sample) [4]Sample {
	num := len(input)

	return [4]Sample{
		input[num-4],
		input[num-3],
		input[num-2],
		input[num-1],
	}
}
