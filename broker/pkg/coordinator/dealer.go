package coordinator

type Dealer interface {
	Shuffle(topicID string, consumers []*Consumer)
}

type BasicDealer struct {
	buckets map[string][]uint16
}

func NewBasicDealer(buckets map[string][]uint16) Dealer {
	dealer := BasicDealer{
		buckets: buckets,
	}

	return dealer
}

func (d BasicDealer) Shuffle(topicID string, consumers []*Consumer) {
	maxBuckets := len(d.buckets)/len(consumers) + 1

	for i, b := range d.buckets[topicID] {
		consumerIndex := i / maxBuckets
		consumers[consumerIndex].AssignedBuckets = append(consumers[consumerIndex].AssignedBuckets, b)
	}
}

type SimpleDealer struct {
	buckets map[string][]uint16
}

func NewSimpleDealer(buckets map[string][]uint16) Dealer {
	dealer := SimpleDealer{
		buckets: buckets,
	}

	return dealer
}

func (d SimpleDealer) Shuffle(topicID string, consumers []*Consumer) {
	maxBuckets := len(d.buckets)/len(consumers) + 1

	// assign dedicated buckets in ascending order
	for i, b := range d.buckets[topicID] {
		consumerIndex := i / maxBuckets
		// assign dedicated buckets in ascending order
		consumers[consumerIndex].AssignedBuckets = append(consumers[consumerIndex].AssignedBuckets, b)
	}
	for i, _ := range d.buckets[topicID] {
		consumerIndex := i / maxBuckets
		// assign shared buckets in descending order to reduce collision probability and help slower consumers
		consumers[consumerIndex].AssignedBuckets = append(consumers[consumerIndex].AssignedBuckets, reverse(d.buckets[topicID][i+1:i+maxBuckets])...)
	}
}

func reverse(s []uint16) []uint16 {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}
