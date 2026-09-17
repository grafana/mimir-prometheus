// Copyright The Prometheus Authors
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/seriesmetadata"
)

func TestCompactionNativeMetadata(t *testing.T) {
	for _, customPostings := range []bool{false, true} {
		for _, shardCount := range []uint64{1, 3} {
			t.Run(fmt.Sprintf("custom postings=%t/shards=%d", customPostings, shardCount), func(t *testing.T) {
				blocks, series := createNativeMetadataCompactionBlocks(t)
				opts := LeveledCompactorOptions{
					EnableNativeMetadata:    true,
					EnableResourceAttrIndex: true,
					IndexedResourceAttrs:    map[string]struct{}{"version": {}},
				}
				if customPostings {
					// A prefix makes these postings incompatible with the raw decoder.
					opts.PE = func(e *encoding.Encbuf, refs []uint32) error {
						e.PutByte(0xff)
						return index.EncodePostingsRaw(e, refs)
					}
					opts.PD = func(meta *BlockMeta) index.PostingsDecoder {
						require.NotEqual(t, ulid.ULID{}, meta.ULID)
						return func(d encoding.Decbuf) (int, index.Postings, error) {
							if d.Byte() != 0xff {
								return 0, nil, errors.New("missing custom postings prefix")
							}
							return index.DecodePostingsRaw(d)
						}
					}
				}
				c, err := NewLeveledCompactorWithOptions(t.Context(), nil, nil, []int64{200}, nil, opts)
				require.NoError(t, err)
				dest := t.TempDir()
				ids, err := c.CompactWithSplitting(dest, []string{blocks[0].Dir(), blocks[1].Dir()}, blocks, shardCount)
				require.NoError(t, err)
				require.Len(t, ids, int(shardCount))
				if shardCount == 3 {
					// The only series assigned to shard 2 was deleted from both inputs.
					require.Equal(t, ulid.ULID{}, ids[2])
				}
				for shard, id := range ids {
					if id == (ulid.ULID{}) {
						continue
					}
					b, err := OpenBlock(nil, filepath.Join(dest, id.String()), nil, opts.PD)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, b.Close()) })
					ir, err := b.Index()
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, ir.Close()) })
					mr, err := b.SeriesMetadata()
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, mr.Close()) })
					k, v := index.AllPostingsKey()
					p, err := ir.Postings(t.Context(), k, v)
					require.NoError(t, err)
					var builder labels.ScratchBuilder
					var hashes []uint64
					for p.Next() {
						require.NoError(t, ir.Series(p.At(), &builder, nil))
						ls := builder.Labels()
						hash := labels.StableHash(ls)
						require.Equal(t, uint64(shard), hash%shardCount)
						hashes = append(hashes, hash)
						vr, ok := mr.GetVersionedResource(hash)
						require.True(t, ok, "metadata missing for output series %s", ls)
						require.Len(t, vr.Versions, 2)
						for input := range 2 {
							ts := int64(input*100 + 10)
							rv, ok := mr.GetResourceAt(hash, ts)
							require.True(t, ok)
							require.Equal(t, seriesmetadata.NewResourceVersion(
								map[string]string{"service.name": ls.Get("instance")},
								map[string]string{"version": strconv.Itoa(input)}, ts, ts,
							), rv)
						}
					}
					require.NoError(t, p.Err())
					expectedSeries := 1
					if shardCount == 1 {
						expectedSeries = 2
					}
					require.Len(t, hashes, expectedSeries)
					require.Equal(t, uint64(expectedSeries), b.Meta().Stats.NumSeries)
					require.Equal(t, uint64(expectedSeries), mr.TotalResources())
					for input := range 2 {
						require.ElementsMatch(t, hashes, mr.LookupResourceAttr("version", strconv.Itoa(input)))
					}
					_, found := mr.GetVersionedResource(labels.StableHash(series[2]))
					require.False(t, found, "deleted series metadata survived compaction")
					require.Equal(t, &BlockSeriesMetadata{
						Enabled: true,
						NamespaceRowCounts: map[string]uint64{
							seriesmetadata.NamespaceResourceTable + "_count":     uint64(2 * expectedSeries),
							seriesmetadata.NamespaceResourceMapping + "_count":   uint64(2 * expectedSeries),
							seriesmetadata.NamespaceResourceAttrIndex + "_count": uint64(3 * expectedSeries),
						},
						IndexedResourceAttrs: []string{"version"},
					}, b.Meta().SeriesMetadata)
				}
			})
		}
	}
}

func TestCompactionNativeMetadataFailureCleanup(t *testing.T) {
	blocks, _ := createNativeMetadataCompactionBlocks(t)
	dest := t.TempDir()
	var published string
	var outputCount int
	c, err := NewLeveledCompactorWithOptions(t.Context(), nil, nil, []int64{200}, nil, LeveledCompactorOptions{
		EnableNativeMetadata: true,
		PD: func(meta *BlockMeta) index.PostingsDecoder {
			outputCount++
			if outputCount == 1 {
				published = filepath.Join(dest, meta.ULID.String(), seriesmetadata.SeriesMetadataFilename)
			} else {
				// Fail the second sidecar write after the first block has been published.
				require.FileExists(t, published)
				blocked := filepath.Join(dest, meta.ULID.String()+tmpForCreationBlockDirSuffix, seriesmetadata.SeriesMetadataFilename+".tmp")
				require.NoError(t, os.Mkdir(blocked, 0o755))
			}
			return index.DecodePostingsRaw
		},
	})
	require.NoError(t, err)
	_, err = c.CompactWithSplitting(dest, []string{blocks[0].Dir(), blocks[1].Dir()}, blocks, 3)
	require.ErrorContains(t, err, "write series metadata file")
	require.Equal(t, 2, outputCount)
	entries, err := os.ReadDir(dest)
	require.NoError(t, err)
	require.Empty(t, entries, "failed compaction left published or temporary output blocks")
}

func createNativeMetadataCompactionBlocks(t *testing.T) ([]*Block, []labels.Labels) {
	t.Helper()
	series := make([]labels.Labels, 3)
	for shard := range series {
		for n := 0; ; n++ {
			ls := labels.FromStrings("__name__", "test_metric", "instance", strconv.Itoa(n))
			if labels.StableHash(ls)%3 == uint64(shard) {
				series[shard] = ls
				break
			}
		}
	}
	var blocks []*Block
	for input := range 2 {
		opts := DefaultHeadOptions()
		opts.ChunkDirRoot = t.TempDir()
		opts.EnableNativeMetadata = true
		h, err := NewHead(nil, nil, nil, nil, opts, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, h.Close()) })
		require.NoError(t, h.Init(0))
		app := h.Appender(t.Context())
		ts := int64(input*100 + 10)
		for _, ls := range series {
			ref, err := app.Append(0, ls, ts, float64(input))
			require.NoError(t, err)
			_, err = app.UpdateResource(ref, ls, map[string]string{"service.name": ls.Get("instance")}, map[string]string{"version": strconv.Itoa(input)}, ts)
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
		c, err := NewLeveledCompactorWithOptions(t.Context(), nil, nil, []int64{200}, nil, LeveledCompactorOptions{EnableNativeMetadata: true})
		require.NoError(t, err)
		dest := t.TempDir()
		ids, err := c.Write(dest, h, int64(input*100), int64((input+1)*100), nil)
		require.NoError(t, err)
		require.Len(t, ids, 1)
		b, err := OpenBlock(nil, filepath.Join(dest, ids[0].String()), nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Close()) })
		require.NoError(t, b.Delete(t.Context(), 0, 200, labels.MustNewMatcher(labels.MatchEqual, "instance", series[2].Get("instance"))))
		blocks = append(blocks, b)
	}
	return blocks, series
}
