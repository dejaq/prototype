package time

import "time"

func GetLatencyMS(ts uint64) int64 {
	now := TimeToMS(time.Now().UTC())
	return int64(now) - int64(ts)
}

func TimeToMS(t time.Time) uint64 {
	return uint64(t.Round(time.Millisecond).UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)))
}

func DurationToMS(d time.Duration) uint64 {
	return uint64(d / time.Millisecond)
}
