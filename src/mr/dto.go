package mr

type RegisterActorCommand struct {
	ActorLocation string
}

func NewRegisteredActorResponse(actorId ActorId) RegisteredActorResponse {
	return RegisteredActorResponse{ActorId: actorId}
}

type RegisteredActorResponse struct {
	ActorId ActorId
}

func NewActorViewModel(actorId ActorId, processId string) ActorViewModel {
	return ActorViewModel{ActorId: actorId, ProcessId: processId}
}

type ActorViewModel struct {
	ActorId   ActorId
	ProcessId string
}

func NewTaskViewModelWhenSetupCoordinator(id TaskId, targetPath string, numberReduce int) TaskViewModel {
	return TaskViewModel{
		Id:              id,
		TaskKind:        TaskKindMap,
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

func NewMapTaskResult(id TaskId, taskKind TaskKind, taskState TaskState, filenameAll []string) TaskResult {
	return TaskResult{
		Id:          id,
		TaskKind:    taskKind,
		TaskState:   taskState,
		filenameAll: filenameAll,
	}
}

type TaskResult struct {
	Id          TaskId
	TaskKind    TaskKind
	TaskState   TaskState
	filenameAll []string
}
