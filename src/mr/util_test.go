package mr

import (
	"testing"
)

func TestGetRangeFromListByCondition(t *testing.T) {
	list := []int{0, 1, 1, 1, 1, 1, 3, 4}
	start := 1
	eq := func(i, j int) bool {
		return list[i] == list[j]
	}
	wantHead := 1
	wantTail := 6

	gotHead, gotTail := GetRangeFromListByCondition(list, start, eq)
	if gotHead != wantHead {
		t.Errorf("GetRangeFromListByCondition() gotHead = %v, want %v", gotHead, wantHead)
	}
	if gotTail != wantTail {
		t.Errorf("GetRangeFromListByCondition() gotTail = %v, want %v", gotTail, wantTail)
	}
}
