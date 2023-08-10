package mr

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

func (t *TaskViewModel) IsIdle() bool {
	return t.TaskState == TaskStateIdle
}

func NewMapTaskResult(id TaskId, taskState TaskState, partitionFilenameAll []string) TaskResult {
	return TaskResult{
		Id:          id,
		TaskKind:    TaskKindMap,
		TaskState:   taskState,
		filenameAll: partitionFilenameAll,
	}
}

func NewReduceTaskResult(id TaskId, taskState TaskState, finalOutputFilename string) TaskResult {
	return TaskResult{
		Id:          id,
		TaskKind:    TaskKindReduce,
		TaskState:   taskState,
		filenameAll: []string{finalOutputFilename},
	}
}

type TaskResult struct {
	Id          TaskId
	TaskKind    TaskKind
	TaskState   TaskState
	filenameAll []string
}

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
