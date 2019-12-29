package coordinator

import (
	"testing"
)

func TestGenerateRanges(t *testing.T) {
	ranges := generateRangesFor(7, 89)
	if len(ranges) != 7 {
		t.Fail()
	}
}

func TestGladiatorDealer_Shuffle(t *testing.T) {
	consumers := []*Consumer{
		&Consumer{},
		&Consumer{},
		&Consumer{},
		&Consumer{},
		&Consumer{},
	}

	NewGladiatorDealer().Shuffle(consumers, 89)

	if len(consumers) != 5 {
		t.Fail()
	}
}
