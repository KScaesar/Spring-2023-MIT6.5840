package mr

import (
	"testing"
)

func TestGetRangeFromListByCondition(t *testing.T) {
	testcase := []struct {
		name     string
		wantHead int
		wantTail int

		list  []int
		start int
	}{
		{
			wantHead: 1,
			wantTail: 6,
			list:     []int{0, 1, 1, 1, 1, 1, 3, 4},
			start:    1,
		},
		{
			wantHead: 6,
			wantTail: 7,
			list:     []int{0, 1, 1, 1, 1, 1, 3, 4},
			start:    6,
		},
		{
			wantHead: 6,
			wantTail: 8,
			list:     []int{0, 1, 1, 1, 1, 1, 4, 4},
			start:    6,
		},
	}

	for _, tt := range testcase {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			eq := func(i, j int) bool {
				return tt.list[i] == tt.list[j]
			}
			gotHead, gotTail := GetRangeFromListByCondition(tt.list, tt.start, eq)
			if gotHead != tt.wantHead {
				t.Errorf("GetRangeFromListByCondition() gotHead = %v, want %v", gotHead, tt.wantHead)
			}
			if gotTail != tt.wantTail {
				t.Errorf("GetRangeFromListByCondition() gotTail = %v, want %v", gotTail, tt.wantTail)
			}
		})
	}
}
