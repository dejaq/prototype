package carrier

import (
	"reflect"
	"testing"

	"github.com/dejaq/prototype/broker/domain"
)

func TestGenerateRanges(t *testing.T) {
	var rangetests = []struct {
		consumers uint16
		buckets   uint16
		result    []domain.BucketRange
	}{
		{0, 10, nil},
		{10, 0, nil},
		{10, 1, []domain.BucketRange{{Start: 0, End: 0}}},
		{1, 10, []domain.BucketRange{{Start: 0, End: 9}}},
		{2, 2, []domain.BucketRange{{0, 0}, {1, 1}}},
		{3, 2, []domain.BucketRange{{0, 0}, {1, 1}}},
		{3, 89, []domain.BucketRange{{0, 29}, {30, 59}, {60, 88}}},
		{4, 4357, []domain.BucketRange{{0, 1089}, {1090, 2179}, {2180, 3269}, {3270, 4356}}},
		{5, 15, []domain.BucketRange{{0, 2}, {3, 5}, {6, 8}, {9, 11}, {12, 14}}},
	}

	for _, tt := range rangetests {
		t.Run("", func(t *testing.T) {
			result := generateRangesFor(tt.consumers, tt.buckets)
			if !reflect.DeepEqual(result, tt.result) {
				t.Errorf("failed for %v wanted=%v got=%v", tt, tt.result, result)
			}
		})
	}
}

func TestGladiatorDealer_Shuffle(t *testing.T) {
	var rangetests = []struct {
		consumers uint16
		buckets   uint16
	}{
		{1, 1},
		{2, 1},
		{1, 2},
		{11, 1},
		{11, 11},
		{89, 1},
		{100, 89},
		{100, 200},
		{99, 9900},
		{1024, 99},
	}
	for _, tt := range rangetests {
		t.Run("", func(t *testing.T) {
			consumers := make([]*Consumer, tt.consumers)
			for i := range consumers {
				consumers[i] = &Consumer{}
			}
			g := NewGladiatorDealer()
			g.Shuffle(consumers, tt.buckets)

			for _, c := range consumers {
				//generate all possible buckets it should have
				leftToCheck := map[uint16]struct{}{}
				for bi := uint16(0); bi < tt.buckets; bi++ {
					leftToCheck[bi] = struct{}{}
				}
				//remove all found buckets, and if one does not match it failed
				//because all buckets has to be assigned to all consumers
				for _, r := range c.assignedBuckets {
					for bi := r.Min(); bi <= r.Max(); bi++ {
						if _, ok := leftToCheck[bi]; ok {
							delete(leftToCheck, bi)
							continue
						}
						t.Errorf("failed duplicate/wrong bucket=%d for test=%v", bi, tt)
					}
				}
				if len(leftToCheck) > 0 {
					t.Errorf("buckets %v left not assigned", leftToCheck)
				}
			}
		})
	}
}
