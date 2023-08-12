//go:build integration_test

package mr

import (
	"strconv"
	"strings"
	"testing"
	"unicode"
)

func TestMapTask_Exec(t1 *testing.T) {
	dto := &TaskViewModel{
		Id:              3,
		TaskKind:        TaskKindMap,
		TaskState:       TaskStateInProgress,
		TargetPath:      []string{"pg-tom_sawyer.txt"},
		NumberReduce:    3,
		AssignedActorId: 0,
	}
	wordCountMapper := func(filename string, contents string) []KeyValue {
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		words := strings.FieldsFunc(contents, ff)
		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}
	task := NewMapTask(dto, wordCountMapper)

	task.Exec()
}

func TestReduceTask_Exec(t1 *testing.T) {
	dto := &TaskViewModel{
		Id:        0,
		TaskKind:  TaskKindReduce,
		TaskState: TaskStateInProgress,
		// TargetPath: []string{"mr-2-0", "mr-1-0", "mr-0-0", "mr-3-0", "mr-7-0", "mr-5-0", "mr-6-0", "mr-4-0"},
		TargetPath: []string{"mr-2-0", "mr-1-0", "mr-0-0", "mr-3-0", "mr-7-0"},
		// TargetPath:      []string{"mr-2-0", "mr-1-0", "mr-0-0"},
		NumberReduce:    2,
		AssignedActorId: 0,
	}
	wordCountReducer := func(key string, values []string) string {
		return strconv.Itoa(len(values))
	}
	task := NewReduceTask(dto, wordCountReducer)

	task.Exec()
}
