package domain

// Client represents a client entity from the server perspective
type Client struct {
	ID       string
	IP       string
	IPv6     string
	Hostname string
}

// BucketRange represents a range of buckets, calculated by dealer
//and used by Storage.
type BucketRange struct {
	MinInclusive uint16
	MaxExclusive uint16
}
