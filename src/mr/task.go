package mr

import (
	"bytes"
)

type TaskId = int

type TaskKind int

const (
	TaskKindMap TaskKind = 1<<iota + 1
	TaskKindReduce
	TaskKindIdle
)

func NewTask(t *TaskViewModel, content []byte) Task {
	return Task{
		Id:        t.Id,
		Filename:  t.Filename,
		Kind:      t.Kind,
		Done:      t.Done,
		beforeRun: content,
	}
}

type Task struct {
	Id        TaskId
	Filename  string
	Kind      TaskKind
	Done      bool
	beforeRun []byte
	afterRun  bytes.Buffer
}

func (t *Task) RunMapper(mapper func(string, string) []KeyValue) {

}

func (t *Task) RunReducer(reducer func(string, []string) string) {

}
