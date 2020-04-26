package domain

import (
	"strconv"
	"strings"
)

// Client represents a client entity from the server perspective
type Client struct {
	ID       string
	IP       string
	IPv6     string
	Hostname string
}

// BucketRange represents a range of buckets, calculated by dealer
// and used by Storage. Start and End are both INCLUSIVE.
// Start:7 End:7 Means only one bucket, 7
// Empty value it is the 0 bucket only.
type BucketRange struct {
	Start uint16
	End   uint16
}

// CSV returns an ordered comma separated list of buckets
func (b BucketRange) CSV() string {
	result := strings.Builder{}
	result.Grow(b.Size() * 3)
	for i := b.Min(); i <= b.Max(); i++ {
		result.WriteString(strconv.Itoa(int(i)))
		if i < b.Max() {
			result.WriteRune(',')
		}
	}
	return result.String()
}

func (b BucketRange) ASC() BucketRange {
	return BucketRange{
		Start: b.Min(),
		End:   b.Max(),
	}
}

func (b BucketRange) DESC() BucketRange {
	return BucketRange{
		Start: b.Max(),
		End:   b.Min(),
	}
}

func (b BucketRange) Min() uint16 {
	if b.Start < b.End {
		return b.Start
	}
	return b.End
}

func (b BucketRange) Max() uint16 {
	if b.Start < b.End {
		return b.End
	}
	return b.Start
}

func (b BucketRange) Size() int {
	return int(b.Max()-b.Min()) + 1
}
