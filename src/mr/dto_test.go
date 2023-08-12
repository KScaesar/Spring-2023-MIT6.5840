package mr

import (
	"testing"
)

func TestTaskResult_ParseReduceId(t *testing.T) {
	result := NewMapTaskResult(1, TaskStateDone, []string{"mr-5-0", "mr-5-1", "mr-5-2"})
	wantReduceIdAll := []int{0, 1, 2}

	gotReduceIdAll := result.ParseReduceIdAll()

	if len(wantReduceIdAll) != len(gotReduceIdAll) {
		t.Errorf("ParseReduceIdAll() gotReduceIdAll = %v, wantReduceIdAll %v", gotReduceIdAll, wantReduceIdAll)
	}
}
