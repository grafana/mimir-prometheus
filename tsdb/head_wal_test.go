package tsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOOOWBLReplayDuration(t *testing.T) {
	dir := "/tmp/test-1-417626"

	opts := DefaultOptions()
	opts.OOOCapMin = 4
	opts.OOOCapMax = 32
	opts.OOOAllowance = 2 * time.Hour.Milliseconds()
	opts.AllowOverlappingQueries = true
	opts.AllowOverlappingCompaction = true

	now := time.Now()

	db, err := Open(dir, nil, nil, opts, nil)
	require.NoError(t, err)
	db.DisableCompactions()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	t.Logf("TSBB Open duration: %s", time.Since(now))
}
