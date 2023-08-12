package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

type TaskId = int

type TaskKind string

const (
	TaskKindMap    TaskKind = "Map"
	TaskKindReduce TaskKind = "Reduce"
)

type TaskState string

const (
	TaskStateIdle       TaskState = "idle"
	TaskStateInProgress TaskState = "wip"
	TaskStateDone       TaskState = "done"
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
	mapper          func(k1 string, v1 string) []KeyValue // map(k1,v1) → list(k2,v2)
}

func (t *MapTask) Run() TaskResult {
	kvAll := t.doMapper()
	partitions := t.shufflePartition(kvAll)
	filenameAll := t.writeIntermediateFile(partitions)
	return NewMapTaskResult(t.id, t.taskState, filenameAll)
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
		tempFilePath := filename + "-temp*"
		filenameAll = append(filenameAll, filename)

		err := AtomicWriteFile(filename, tempFilePath, func(file *os.File) error {
			b, _ := json.Marshal(partition)
			_, err := file.Write(b)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Fatalf("AtomicWriteFile: %v", err)
			return
		}
	}
	t.taskState = TaskStateDone
	return
}

func NewReduceTask(
	dto *TaskViewModel,
	reducer func(string, []string) string,
) *ReduceTask {
	return &ReduceTask{
		id:              dto.Id,
		taskKind:        TaskKindReduce,
		taskState:       TaskStateInProgress,
		targetPathAll:   dto.TargetPath,
		assignedActorId: dto.AssignedActorId,
		reducer:         reducer,
	}
}

type ReduceTask struct {
	id            TaskId
	taskKind      TaskKind
	taskState     TaskState
	targetPathAll []string

	assignedActorId int
	reducer         func(k2 string, v2All []string) string // reduce(k2,list(v2)) → list(v2)
}

func (t *ReduceTask) Run() TaskResult {
	keys, results := t.doReducer()
	filename := t.writeResultFile(keys, results)
	return NewReduceTaskResult(t.id, t.taskState, filename)
}

func (t *ReduceTask) doReducer() (keys []string, results map[string]string) {
	const defaultSize = 1000
	payload := make([]KeyValue, 0, defaultSize)
	key := ""
	values := make([]string, 0, defaultSize)
	cursor := 0

	keys = make([]string, 0, defaultSize)
	pairs := make(map[string][]string)
	for _, path := range t.targetPathAll {
		data := OpenLocalFile(path)
		json.Unmarshal(data, &payload)

		for cursor < len(payload) {
			head, tail := GetRangeFromListByCondition(payload, cursor, func(i, j int) bool {
				return payload[i].Key == payload[j].Key
			})

			key = payload[head].Key
			for _, kv := range payload[head:tail] {
				values = append(values, kv.Value)
			}
			keys = append(keys, key)
			pairs[key] = append(pairs[key], values...)
			cursor = tail
		}

		// reset
		payload = payload[:0]
		key = ""
		values = values[:0]
		cursor = 0
	}

	results = make(map[string]string, len(pairs))
	for key, values = range pairs {
		result := t.reducer(key, values)
		results[key] = result
	}

	return keys, results
}

func (t *ReduceTask) writeResultFile(keys []string, results map[string]string) (filename string) {
	filename = "mr-out-" + strconv.Itoa(t.id)
	tempFilePath := filename + "-temp*"

	builder := bytes.Buffer{}
	err := AtomicWriteFile(filename, tempFilePath, func(file *os.File) error {
		for _, key := range keys {
			builder.WriteString(key)
			builder.WriteString(" ")
			builder.WriteString(results[key])
			builder.WriteString("\n")
			_, err := file.Write(builder.Bytes())
			if err != nil {
				return err
			}
			builder.Reset()
		}
		return nil
	})
	if err != nil {
		log.Fatalf("AtomicWriteFile: %v", err)
		return
	}

	t.taskState = TaskStateDone
	return
}
