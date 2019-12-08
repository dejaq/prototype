package coordinator

import (
	"math"

	"github.com/dejaq/prototype/broker/domain"
)

// Dealer assigns buckets to consumer (mutates the input !) in a deterministic way
type Dealer interface {
	Shuffle(consumers []*Consumer, noOfBuckets uint16)
}

// generateRangesFor generates a list of ranges covering all buckets, to be evenly split
// to a number of consumers. Returns nil for 0 consumers
// for 3 consumers and 10 buckets will do: [0,3),[3,6),[7,10)
func generateRangesFor(consumersCount uint16, noOfBuckets uint16) []domain.BucketRange {
	result := make([]domain.BucketRange, 0, consumersCount)

	if consumersCount <= 0 || noOfBuckets <= 0 {
		return nil
	}

	//1 consumer 1 range
	if consumersCount == 1 || noOfBuckets == 1 {
		result = append(result, domain.BucketRange{
			Start: 0,
			End:   noOfBuckets - 1,
		})
		return result
	}
	var i uint16

	if consumersCount >= noOfBuckets {
		// 1 consumer 1 bucket OR
		// more consumers than buckets, some of them will have no buckets!
		for ; i < consumersCount; i++ {
			result = append(result, domain.BucketRange{
				Start: i,
				End:   i,
			})
		}
		return result
	}

	//most likely case, 1 consumer multiple buckets
	avgCountPerConsumer := uint16(math.Ceil(float64(noOfBuckets) / float64(consumersCount)))
	latestI := consumersCount - 1

	for ; i < consumersCount; i++ {
		r := domain.BucketRange{
			Start: i * avgCountPerConsumer,
		}
		if latestI == i {
			//this covers the case when consumers=3 buckets=11 and last range should be [7,11)
			r.End = noOfBuckets
		} else {
			r.End = r.Start + avgCountPerConsumer - 1
		}
		result = append(result, r)
	}

	return result
}

// ExclusiveDealer splits each range to each consumer having no overlap.
// Meaning ech consumer will have exclusivity on its assigned ranges (no collisions, no fallback)
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
			// if it does not have a range, just reset it
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
	noOfConsumers := uint16(len(consumers))
	allRanges := generateRangesFor(noOfConsumers, noOfBuckets)

	noOfRanges := uint16(len(allRanges))
	// assign ranges to customers in ascending order
	for i := range consumers {
		if i < len(allRanges) {
			consumers[i].AssignedBuckets = []domain.BucketRange{allRanges[i]}
		} else {
			// if it does not have a range, just reset it
			consumers[i].AssignedBuckets = nil
		}
	}

	for i := 0; i < int(noOfConsumers)*int(noOfRanges-1); i++ {
		consumerIndex := (i + int(noOfRanges)) % int(noOfConsumers)
		consumers[consumerIndex].AssignedBuckets = append(consumers[consumerIndex].AssignedBuckets, allRanges[(i+int(noOfRanges))/int(noOfRanges)].DESC())
	}
}
