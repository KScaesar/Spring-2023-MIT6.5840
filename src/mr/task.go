package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
)

type TaskId = int

type TaskKind int

const (
	TaskKindMap TaskKind = 1<<iota + 1
	TaskKindReduce
)

type TaskState int

const (
	TaskStateIdle TaskState = 1<<iota + 1
	TaskStateInProgress
	TaskStateDone
)

type Task interface {
	Run() TaskResult
}

func NewMapTask(
	dto *TaskViewModel,
	mapper func(string, string) []KeyValue,
) *MapTask {
	return &MapTask{
		id:              dto.Id,
		taskKind:        TaskKindMap,
		taskState:       TaskStateInProgress,
		targetPathAll:   dto.TargetPath,
		assignedActorId: dto.AssignedActorId,
		numberReduce:    dto.NumberReduce,
		mapper:          mapper,
	}
}

type MapTask struct {
	id            TaskId
	taskKind      TaskKind
	taskState     TaskState
	targetPathAll []string

	assignedActorId int
	numberReduce    int
	mapper          func(string, string) []KeyValue
}

func (t *MapTask) Run() TaskResult {
	kvAll := t.doMapper()
	partitions := t.shufflePartition(kvAll)
	filenameAll := t.writeIntermediateFile(partitions)
	return NewMapTaskResult(t.id, t.taskKind, t.taskState, filenameAll)
}

func (t *MapTask) doMapper() (output []KeyValue) {
	for _, path := range t.targetPathAll {
		content := OpenLocalFile(path)
		kv := t.mapper(path, string(content))
		output = append(output, kv...)
	}
	return
}

func (t *MapTask) shufflePartition(kvAll []KeyValue) [][]KeyValue {
	sort.Slice(kvAll, func(i, j int) bool {
		return kvAll[i].Key < kvAll[j].Key
	})

	partitions := make([][]KeyValue, t.numberReduce)
	cursor := 0
	for cursor < len(kvAll) {
		head, tail := GetRangeFromListByCondition(kvAll, cursor, func(i, j int) bool {
			return kvAll[i].Key == kvAll[j].Key
		})
		key := kvAll[cursor].Key
		reduceTaskId := ihash(key) % t.numberReduce
		// fmt.Println("key", key, "head", head, "tail", tail)
		partitions[reduceTaskId] = append(partitions[reduceTaskId], kvAll[head:tail]...)
		cursor = tail
	}

	return partitions
}

func (t *MapTask) writeIntermediateFile(partitions [][]KeyValue) (filenameAll []string) {
	for reduceId, partition := range partitions {
		filename := fmt.Sprintf("mr-%v-%v", t.id, reduceId)
		filenameAll = append(filenameAll, filename)

		_, err := os.Stat("./" + filename)
		if err == nil {
			log.Printf("file='%v' exist\n", filename)
			continue
		}

		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("query file for check task is completed: %v\n", err)
			return
		}

		// MapReduce paper mentions the trick of
		// using a temporary file and atomically renaming it
		// once it is completely written.
		file, err := os.CreateTemp("./", fmt.Sprintf("mr-%v-temp*", t.id))
		if err != nil {
			log.Fatalf("create file failed: %v\n", err)
			return
		}

		tempName := file.Name()
		b, _ := json.Marshal(partition)
		_, err = file.Write(b)
		if err != nil {
			log.Fatalf("write intermediate file failed: %v\n", err)
			return
		}
		file.Close()

		err = os.Rename(tempName, filename)
		if err != nil {
			panic(fmt.Errorf("rename file failed: %v", err))
			return
		}
	}
	t.taskState = TaskStateDone
	return
}

type ReduceTask struct {
	id            TaskId
	taskKind      TaskKind
	taskState     TaskState
	targetPathAll []string

	assignedActorId int
	reducer         func(string, []string) string
}
