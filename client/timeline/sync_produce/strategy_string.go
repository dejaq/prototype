// Code generated by "stringer -type=Strategy"; DO NOT EDIT.

package sync_produce

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[StrategySingleBurst-0]
	_ = x[StrategyConstantBursts-1]
}

const _Strategy_name = "StrategySingleBurstStrategyConstantBursts"

var _Strategy_index = [...]uint8{0, 19, 41}

func (i Strategy) String() string {
	if i >= Strategy(len(_Strategy_index)-1) {
		return "Strategy(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Strategy_name[_Strategy_index[i]:_Strategy_index[i+1]]
}