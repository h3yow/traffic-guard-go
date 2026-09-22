package guard

// Logger is the minimal logging sink used for per-request decision lines.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// LogDecision prints request-dimension inputs, quota/window state and the
// basis for the decision in one stable, greppable line.
func LogDecision(log Logger, d Decision) {
	if log == nil {
		return
	}
	log.Printf(
		"decision t=%s client=%q outcome=%s accepted=%v | quota: tokens=%.3f cap=%.0f clientInFlight=%d/%d globalInFlight=%d/%d | circuit: state=%s consecFailures=%d failKind=%q | basis=%s",
		d.Time.UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		d.Client, d.Outcome, d.Accepted,
		d.Quota.TokensAvail, d.Quota.BucketCap,
		d.Quota.InFlight, d.Quota.BurstLimit,
		d.Quota.GlobalInFlight, d.Quota.GlobalLimit,
		d.Circuit, d.ConsecFail, d.FailKind, d.Detail,
	)
}
