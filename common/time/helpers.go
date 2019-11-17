package time

import "time"

func TimeToMS(t time.Time) uint64 {
	return uint64(t.Round(time.Millisecond).UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)))
}

func DurationToMS(d time.Duration) uint64 {
	return uint64(d / time.Millisecond)
}
