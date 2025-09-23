// Copyright 2021 Grafana Labs
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

package tsdb

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSymbolsBatchAndIteration1(t *testing.T) {
	testSymbolsBatchAndIterationWithFlushersConcurrency(t, 1)
}

func TestSymbolsBatchAndIteration5(t *testing.T) {
	testSymbolsBatchAndIterationWithFlushersConcurrency(t, 5)
}

func testSymbolsBatchAndIterationWithFlushersConcurrency(t *testing.T, flushersConcurrency int) {
	flushers := newSymbolFlushers(flushersConcurrency)
	defer func() { _ = flushers.close() }()

	dir := t.TempDir()

	b := newSymbolsBatcher(100, dir, flushers)

	allWords := map[string]struct{}{}

	for i := 0; i < 10*flushersConcurrency; i++ {
		require.NoError(t, b.addSymbol(""))
		allWords[""] = struct{}{}

		for j := 0; j < 123; j++ {
			w := fmt.Sprintf("word_%d_%d", i%3, j)

			require.NoError(t, b.addSymbol(w))

			allWords[w] = struct{}{}
		}
	}

	require.NoError(t, b.flushSymbols(true))
	require.NoError(t, b.flushSymbols(true)) // call again, this should do nothing, and not create new empty file.
	require.NoError(t, flushers.close())

	symbols := b.getSymbolFiles()

	it, err := newSymbolsIterator(symbols)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, it.Close())
	})

	first := true
	var w, prev string
	for w, err = it.NextSymbol(); err == nil; w, err = it.NextSymbol() {
		if !first {
			require.NotEmpty(t, w)
			require.Less(t, prev, w)
		}

		first = false

		_, known := allWords[w]
		require.True(t, known)
		delete(allWords, w)
		prev = w
	}
	require.Equal(t, io.EOF, err)
	require.Empty(t, allWords)
}
