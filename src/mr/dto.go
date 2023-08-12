package mr

import (
	"log"
	"strconv"
	"strings"
)

func NewRegisterActorCommand(actorLocation string) RegisterActorCommand {
	return RegisterActorCommand{ActorLocation: actorLocation}
}

type RegisterActorCommand struct {
	ActorLocation string
}

func NewRegisteredActorResponse(actorId ActorId) RegisteredActorResponse {
	return RegisteredActorResponse{ActorId: actorId}
}

type RegisteredActorResponse struct {
	ActorId ActorId
}

func NewMapTaskViewModelWhenSetupCoordinator(id TaskId, targetPath string, numberReduce int) TaskViewModel {
	return TaskViewModel{
		Id:              id,
		TaskKind:        TaskKindMap,
		TaskState:       TaskStateIdle,
		TargetPath:      []string{targetPath},
		NumberReduce:    numberReduce,
		AssignedActorId: -1,
	}
}

func NewReduceTaskViewModelWhenMapTaskDone(id TaskId, targetPath string, numberReduce int) TaskViewModel {
	return TaskViewModel{
		Id:              id,
		TaskKind:        TaskKindReduce,
		TaskState:       TaskStateIdle,
		TargetPath:      []string{targetPath},
		NumberReduce:    numberReduce,
		AssignedActorId: -1,
	}
}

type TaskViewModel struct {
	Id              TaskId
	TaskKind        TaskKind
	TaskState       TaskState
	TargetPath      []string
	NumberReduce    int
	AssignedActorId int
}

func (t *TaskViewModel) IsIdle() bool {
	return t.TaskState == TaskStateIdle
}

func NewMapTaskResult(id TaskId, taskState TaskState, partitionFilenameAll []string) TaskResult {
	return TaskResult{
		Id:          id,
		TaskKind:    TaskKindMap,
		TaskState:   taskState,
		FilenameAll: partitionFilenameAll,
	}
}

func NewReduceTaskResult(id TaskId, taskState TaskState, finalOutputFilename string) TaskResult {
	return TaskResult{
		Id:          id,
		TaskKind:    TaskKindReduce,
		TaskState:   taskState,
		FilenameAll: []string{finalOutputFilename},
	}
}

type TaskResult struct {
	Id          TaskId
	TaskKind    TaskKind
	TaskState   TaskState
	FilenameAll []string
}

func (t *TaskResult) ParseReduceIdAll() (reduceIdAll []int) {
	if t.TaskKind == TaskKindMap {
		for _, filename := range t.FilenameAll {
			splitN := strings.SplitN(filename, "-", 3)
			reduceId, _ := strconv.Atoi(splitN[2])
			reduceIdAll = append(reduceIdAll, reduceId)
		}
		return
	}
	log.Fatalln("Task Kind must be Map when ParseReduceIdAll")
	return nil
}

type TaskResultResponse struct{}

func NewAcquireTaskCommand(actorId ActorId) AcquireTaskCommand {
	return AcquireTaskCommand{ActorId: actorId}
}

type AcquireTaskCommand struct {
	ActorId ActorId
}

func NewNotAcquiredTaskResponse() AcquiredTaskResponse {
	return AcquiredTaskResponse{}
}

func NewNormalAcquiredTaskResponse(task *TaskViewModel) AcquiredTaskResponse {
	return AcquiredTaskResponse{
		Task:           *task,
		IsTaskAcquired: true,
	}
}

type AcquiredTaskResponse struct {
	Task           TaskViewModel
	IsTaskAcquired bool
}
