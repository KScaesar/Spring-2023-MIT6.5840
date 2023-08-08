package mr

type RegisterActorCommand struct{}

func NewCreatedActorResponse(actorId ActorId, task *TaskViewModel) RegisteredActorResponse {
	return RegisteredActorResponse{ActorId: actorId, Task: *task}
}

type RegisteredActorResponse struct {
	ActorId ActorId
	Task    TaskViewModel
}

func NewTaskViewModelWhenSetupCoordinator(id TaskId, filename string) TaskViewModel {
	return TaskViewModel{
		Id:       id,
		Filename: filename,
		Kind:     TaskKindMap,
		Done:     false,
	}
}

func NewIdleTaskViewModel(id TaskId) TaskViewModel {
	return TaskViewModel{
		Id:   id,
		Kind: TaskKindIdle,
		Done: false,
	}
}

type TaskViewModel struct {
	Id       TaskId
	Filename string
	Kind     TaskKind
	Done     bool
}
