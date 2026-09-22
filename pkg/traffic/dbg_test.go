package traffic

import (
	"testing"
	"time"
)

func TestDbgCounts(t *testing.T) {
	cfg := testCfg()
	_ = cfg
	cs := &clientState{}
	now := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	add := func(offset time.Duration, n int) {
		for i := 0; i < n; i++ {
			cs.admitted = append(cs.admitted, now.Add(offset))
		}
	}
	add(0, 3)
	add(150*time.Millisecond, 3)
	add(450*time.Millisecond, 3)
	add(600*time.Millisecond, 1)
	at := now.Add(1100 * time.Millisecond)
	cs.prune(at.Add(-RateWindow))
	t.Logf("remaining after prune = %d (want 7)", len(cs.admitted))
	for _, ts := range cs.admitted {
		t.Logf("  %s", ts.Sub(now))
	}
}
