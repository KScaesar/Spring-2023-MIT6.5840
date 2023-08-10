package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type CoordinatorState int

const (
	CoordinatorStateMap CoordinatorState = 1<<iota + 1
	CoordinatorStateReduce
)

type Coordinator struct {
	NewActorId func() ActorId
	LivedActor map[ActorId]bool

	MapTasks           map[TaskId]TaskViewModel
	MapTaskIdleSize    int
	MapTaskDoneSize    int
	ReduceTasks        map[TaskId]TaskViewModel
	ReduceTaskIdleSize int
	ReduceTaskDoneSize int
	NewReduceId        func() TaskId
	State              CoordinatorState

	mu sync.Mutex
}
func (c *Coordinator) AcquiredTask(command *AcquireTaskCommand, resp *AcquiredTaskResponse) error {
	log.Printf("AcquireTaskCommand: %#v\n", command)
	defer log.Printf("AcquiredTaskResponse: %#v\n", resp)
	c.mu.Lock()

	var task TaskViewModel
	var err error
	switch c.State {
	case CoordinatorStateMap:
		task, err = c.assignTask(command.ActorId, &c.MapTaskIdleSize, c.MapTasks)
	case CoordinatorStateReduce:
		task, err = c.assignTask(command.ActorId, &c.ReduceTaskIdleSize, c.ReduceTasks)
	}
	c.mu.Unlock()

	if err != nil {
		*resp = NewNotAcquiredTaskResponse()
		return nil
	}
	*resp = NewNormalAcquiredTaskResponse(&task)
	return nil
}

func (c *Coordinator) assignTask(actorId ActorId, idleSize *int, tasks map[TaskId]TaskViewModel) (TaskViewModel, error) {
	if *idleSize == 0 {
		return TaskViewModel{}, ErrNoTask
	}

	for taskId, task := range tasks {
		if !task.IsIdle() {
			continue
		}

		task.AssignedActorId = actorId
		task.TaskState = TaskStateInProgress
		*idleSize--
		tasks[taskId] = task
		return task, nil
	}

	return TaskViewModel{}, ErrNoTask
}

func (c *Coordinator) RegisterActor(_ *RegisterActorCommand, resp *RegisteredActorResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	actorId := c.NewActorId()
	c.LivedActor[actorId] = true
	*resp = NewRegisteredActorResponse(actorId)
	log.Printf("RegisterActor: actorId=%v\n", actorId)
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	actorId := -1
	NewActorId := func() ActorId {
		actorId++
		return actorId
	}

	reduceId := -1
	NewReduceId := func() ActorId {
		reduceId++
		return reduceId
	}

	fileQty := len(files)
	tasks := make(map[TaskId]TaskViewModel)
	for i := 0; i < fileQty; i++ {
		id := i
		tasks[id] = NewTaskViewModelWhenSetupCoordinator(id, files[i], nReduce)
	}

	c := Coordinator{
		NewActorId:         NewActorId,
		LivedActor:         make(map[ActorId]bool),
		MapTasks:           tasks,
		MapTaskIdleSize:    fileQty,
		MapTaskDoneSize:    0,
		ReduceTasks:        make(map[TaskId]TaskViewModel, nReduce),
		ReduceTaskIdleSize: nReduce,
		ReduceTaskDoneSize: 0,
		NewReduceId:        NewReduceId,
		State:              CoordinatorStateMap,
		mu:                 sync.Mutex{},
	}

	log.Printf("coordinator run: pid=%v\n", os.Getpid())
	c.server()
	return &c
}
