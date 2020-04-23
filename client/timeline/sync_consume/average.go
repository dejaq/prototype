package sync_consume

import "time"

type Average struct {
	sum float64
	n   int
}

func (a *Average) Add(x time.Duration) {
	if x <= 0 {
		return
	}
	a.sum += float64(x)
	a.n++
}
func (a *Average) Get() time.Duration {
	if a.n > 0 {
		return time.Duration(a.sum / float64(a.n))
	}
	return 0
}
