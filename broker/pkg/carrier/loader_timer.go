package carrier

import "time"

// LoaderTimerConfig contains the settings for the interval between 2 ticks of the Loader
type LoaderTimerConfig struct {
	// the minimum wait time between 2 ticks, will be used when consumers have not consumed all their messages
	// for the last few ticks
	Min time.Duration
	// will reach this state when consumers are up to date, most likely no traffic is done
	Max time.Duration
	// each step the tick wait time is adjusted with this value
	Step time.Duration
}

type loaderTimer struct {
	conf    LoaderTimerConfig
	Current time.Duration
}

func newTimer(conf LoaderTimerConfig) *loaderTimer {
	return &loaderTimer{
		conf:    conf,
		Current: conf.Min,
	}
}

func (t *loaderTimer) GetNextDuration() time.Duration {
	return t.Current
}

func (t *loaderTimer) Decrease() {
	t.Current = t.Current - t.conf.Step
	if t.Current < t.conf.Min {
		t.Current = t.conf.Min
	}
}

func (t *loaderTimer) Increase() {
	t.Current = t.Current + t.conf.Step
	if t.Current > t.conf.Max {
		t.Current = t.conf.Max
	}
}
