package labels

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrieMatches(t *testing.T) {
	actualLabelValues := []string{
		"node_cpu_seconds_total",
		"node_memory_MemAvailable_bytes",
		"node_memory_MemFree_bytes",
		"node_memory_MemTotal_bytes",
		"node_memory_SwapFree_bytes",
		"node_memory_SwapTotal_bytes",
		"node_memory_active_bytes",
		"node_memory_buffers_bytes",
		"node_memory_cached_bytes",
		"node_memory_inactive_bytes",
		"node_memory_mapped_file_bytes",
		"node_memory_page_tables_bytes",
		"node_memory_slab_bytes",
		"node_memory_writeback_bytes",
		"java_lang_Memory_HeapMemoryUsage_committed",
		"java_lang_Memory_HeapMemoryUsage_init",
		"java_lang_Memory_HeapMemoryUsage_max",
		"java_lang_Memory_HeapMemoryUsage_used",
		"java_lang_Memory_NonHeapMemoryUsage_committed",
		"java_lang_Memory_NonHeapMemoryUsage_init",
		"go_memstats_alloc_bytes",
		"go_memstats_alloc_bytes_total",
		"go_memstats_buck_hash_sys_bytes",
		"go_memstats_frees_total",
		"go_memstats_gc_cpu_fraction",
		"go_memstats_gc_sys_bytes",
		"go_memstats_heap_alloc_bytes",
		"go_memstats_heap_idle_bytes",
		"summary_metric_sum",
	}

	nonexistentLabelValues := []string{
		// Values with a shared prefix
		"node_cpu_cfs_periods_total",
		"node_cpu_cfs_throttled_seconds_total",
		"summary_metric_count",
		"summary_metric",

		// Values with no shared prefix
		"ruby_gc_stat_count",
		"ruby_gc_stat_heap_allocated_pages",
	}

	// Shuffle the label values to ensure that the trie is built in a random order.
	rand.Shuffle(len(actualLabelValues), func(i, j int) {
		actualLabelValues[i], actualLabelValues[j] = actualLabelValues[j], actualLabelValues[i]
	})

	tr := &trie{}

	for _, name := range actualLabelValues {
		tr.add(name)
	}

	for _, name := range actualLabelValues {
		require.True(t, tr.Matches(name), "name %s not found in trie", name)
	}

	for _, name := range nonexistentLabelValues {
		require.False(t, tr.Matches(name), "name %s found in trie", name)
	}
}
