package chunkenc

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type infoPair struct {
	t   int64
	ils []int
}

func TestInfoSampleChunk(t *testing.T) {
	c := NewInfoSampleChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	var exp []infoPair
	ts := int64(1234123324)
	for i := 0; i < 300; i++ {
		ts += int64(rand.Intn(10000) + 1)
		var ils []int
		if i%2 == 0 {
			ils = []int{0, 1}
		} else {
			ils = []int{2, 3}
		}

		// Start with a new appender every 10th sample. This emulates starting
		// appending to a partially filled chunk.
		if i%10 == 0 {
			app, err = c.Appender()
			require.NoError(t, err)
		}

		app.AppendInfoSample(ts, ils)
		exp = append(exp, infoPair{t: ts, ils: ils})
		require.Equal(t, i+1, c.NumSamples())
	}
	require.Equal(t, 300, c.NumSamples())
	require.Len(t, exp, 300)

	// 1. Expand iterator in simple case.
	it1 := c.Iterator(nil)
	var res1 []infoPair
	for it1.Next() == ValInfoSample {
		ts, ils := it1.AtInfoSample()
		res1 = append(res1, infoPair{t: ts, ils: ils})
	}
	require.NoError(t, it1.Err())
	require.Len(t, res1, len(exp))
	require.Equal(t, exp[1], res1[1])
	require.Equal(t, exp, res1)

	// 2. Expand second iterator while reusing first one.
	it2 := c.Iterator(it1)
	var res2 []infoPair
	for it2.Next() == ValInfoSample {
		ts, ils := it2.AtInfoSample()
		res2 = append(res2, infoPair{t: ts, ils: ils})
	}
	require.NoError(t, it2.Err())
	require.Equal(t, exp, res2)

	// 3. Test iterator Seek.
	mid := len(exp) / 2

	it3 := c.Iterator(nil)
	var res3 []infoPair
	require.Equal(t, ValInfoSample, it3.Seek(exp[mid].t))
	// Below ones should not matter.
	require.Equal(t, ValInfoSample, it3.Seek(exp[mid].t))
	require.Equal(t, ValInfoSample, it3.Seek(exp[mid].t))
	ts, ils := it3.AtInfoSample()
	res3 = append(res3, infoPair{t: ts, ils: ils})

	for it3.Next() == ValInfoSample {
		ts, ils := it3.AtInfoSample()
		res3 = append(res3, infoPair{t: ts, ils: ils})
	}
	require.NoError(t, it3.Err())
	require.Equal(t, exp[mid:], res3)
	require.Equal(t, ValNone, it3.Seek(exp[len(exp)-1].t+1))
}
