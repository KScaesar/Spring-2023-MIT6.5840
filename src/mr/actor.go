package mr

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type ActorId = int

func NewActor(
	id ActorId,
	processId int,
	mapper func(string, string) []KeyValue,
	reducer func(string, []string) string,
) *Actor {
	return &Actor{
		Id:        id,
		ProcessId: processId,
		mapper:    mapper,
		reducer:   reducer,
	}
}

type Actor struct {
	Id        ActorId
	ProcessId int
	mapper    func(string, string) []KeyValue
	reducer   func(string, []string) string
}

func (a *Actor) Run() {
	log.Printf("Actor run: actorId=%v, processId=%v\n", a.Id, a.ProcessId)
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			task, err := a.AcquireTask()
			if err != nil {
				if errors.Is(err, ErrNoTask) {
					continue
				}
				log.Fatalln(err.Error())
			}
			result := task.Run()
			a.ReportTaskResult(&result)
		}
	}
}

func (a *Actor) ReportTaskResult(r *TaskResult) {
	resp := TaskResultResponse{}
	ok := call("Coordinator.ReportTaskResult", r, &resp)
	if !ok {
		log.Fatalln("Coordinator.ReportTaskResult fail")
		return
	}
	return
}

func (a *Actor) AcquireTask() (Task, error) {
	command := NewAcquireTaskCommand(a.Id)
	resp := AcquiredTaskResponse{}
	ok := call("Coordinator.AcquiredTask", &command, &resp)
	if !ok {
		return nil, fmt.Errorf("call Coordinator.AcquiredTask failed: %w", ErrSystemFail)
	}
	log.Printf("AcquireTask response: %#v\n", resp)

	if !resp.IsTaskAcquired {
		return nil, ErrNoTask
	}

	task, err := a.createTask(&resp.Task)
	if err != nil {
		return nil, fmt.Errorf("createTask: %w", err)
	}
	return task, nil
}

func (a *Actor) createTask(dto *TaskViewModel) (Task, error) {
	switch dto.TaskKind {
	case TaskKindMap:
		return NewMapTask(dto, a.mapper), nil
	case TaskKindReduce:
		return NewReduceTask(dto, a.reducer), nil
	default:
		return nil, fmt.Errorf("task kind only is Map or Reduce: %w", ErrParamNotMatch)
	}
}
