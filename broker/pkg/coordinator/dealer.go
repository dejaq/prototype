package coordinator

import (
	"math"

	"github.com/bgadrian/dejaq-broker/broker/domain"
)

// Dealer assigns buckets to consumer (mutates the input !) in a deterministic way
type Dealer interface {
	Shuffle(consumers []*Consumer, noOfBuckets uint16)
}

// generateRangesFor generates a list of ranges covering all buckets, to be evenly split
//to a number of consumers. Returns nil for 0 consumers
//for 3 consumers and 10 buckets will do: [0,3),[3,6),[7,10)
func generateRangesFor(consumersCount uint16, noOfBuckets uint16) []domain.BucketRange {
	result := make([]domain.BucketRange, 0, consumersCount)

	if consumersCount <= 0 || noOfBuckets <= 0 {
		return nil
	}

	//1 consumer 1 range
	if consumersCount == 1 || noOfBuckets == 1 {
		result = append(result, domain.BucketRange{
			MinInclusive: 0,
			MaxExclusive: noOfBuckets,
		})
		return result
	}
	var i uint16

	if consumersCount >= noOfBuckets {
		//1 consumer 1 bucket OR
		// more consumers than buckets, some of them will have no buckets!
		for ; i < consumersCount; i++ {
			result = append(result, domain.BucketRange{
				MinInclusive: i,
				MaxExclusive: i + 1,
			})
		}
		return result
	}

	//most likely case, 1 consumer multiple buckets
	avgCountPerConsumer := uint16(math.Ceil(float64(noOfBuckets) / float64(consumersCount)))
	latestI := consumersCount - 1

	for ; i < consumersCount; i++ {
		r := domain.BucketRange{
			MinInclusive: i * avgCountPerConsumer,
		}
		if latestI == i {
			//this covers the case when consumers=3 buckets=11 and last range should be [7,11)
			r.MaxExclusive = noOfBuckets
		} else {
			r.MaxExclusive = r.MinInclusive + avgCountPerConsumer
		}
		result = append(result, r)
	}

	return result
}

// ExclusiveDealer splits each range to each consumer having no overlap.
//Meaning ech consumer will have exclusivity on its assigned ranges (no collisions, no fallback)
type ExclusiveDealer struct {
}

func NewExclusiveDealer() Dealer {
	return ExclusiveDealer{}
}

func (d ExclusiveDealer) Shuffle(consumers []*Consumer, noOfBuckets uint16) {
	noOfConsumers := uint16(len(consumers))
	allRanges := generateRangesFor(noOfConsumers, noOfBuckets)

	for i := range consumers {
		if i < len(allRanges) {
			consumers[i].AssignedBuckets = []domain.BucketRange{allRanges[i]}
		} else {
			//if it does not have a range, just reset it
			consumers[i].AssignedBuckets = nil
		}
	}
}

type GladiatorDealer struct {
}

func NewGladiatorDealer() Dealer {
	return GladiatorDealer{}
}

func (d GladiatorDealer) Shuffle(consumers []*Consumer, noOfBuckets uint16) {
	//maxBuckets := len(d.buckets)/len(consumers) + 1

	//// assign dedicated bucketCount in ascending order
	//for i, b := range d.buckets[topicID] {
	//	consumerIndex := i / maxBuckets
	//	// assign dedicated bucketCount in ascending order
	//	consumers[consumerIndex].AssignedBuckets = append(consumers[consumerIndex].AssignedBuckets, b)
	//}
	//for i, _ := range d.buckets[topicID] {
	//	consumerIndex := i / maxBuckets
	//	// assign shared bucketCount in descending order to reduce collision probability and help slower consumers
	//	consumers[consumerIndex].AssignedBuckets = append(consumers[consumerIndex].AssignedBuckets, reverse(d.buckets[topicID][i+1:i+maxBuckets])...)
	//}
}

func reverse(s []uint16) []uint16 {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}
