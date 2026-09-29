package traffic

import (
	"testing"
	"time"
)

func TestDbgCounts(t *testing.T) {
	cfg := testCfg()
	_ = cfg
	cs := &actorState{}
	now := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	add := func(offset time.Duration, n int) {
		for i := 0; i < n; i++ {
			cs.entries = append(cs.entries, costEntry{at: now.Add(offset), cost: 1})
		}
	}
	add(0, 3)
	add(150*time.Millisecond, 3)
	add(450*time.Millisecond, 3)
	add(600*time.Millisecond, 1)
	at := now.Add(1100 * time.Millisecond)
	cs.prune(at.Add(-RateWindow))
	t.Logf("remaining after prune = %d (want 7)", len(cs.entries))
	for _, e := range cs.entries {
		t.Logf("  %s", e.at.Sub(now))
	}
}
