package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

type CoordinatorState int

const (
	CoordinatorStateMap CoordinatorState = 1<<iota + 1
	CoordinatorStateReduce
)

type Coordinator struct {
	NewActorId  func() ActorId
	LivedActors map[ActorId]bool

	MapTasks        map[TaskId]TaskViewModel
	MapTaskSize     int
	MapTaskIdleSize int
	MapTaskDoneSize int

	ReduceTasks        map[TaskId]TaskViewModel
	ReduceTaskSize     int
	ReduceTaskIdleSize int
	ReduceTaskDoneSize int

	State CoordinatorState
	done  atomic.Bool

	mu sync.Mutex
}

func (c *Coordinator) ReportTaskResult(result *TaskResult, _ *TaskResultResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("ReportTaskResult input: %#v\n", result)
	defer log.Printf(
		"ReportTaskResult: state=%v  map{sum=%v idle=%v done=%v} reduce{sum=%v idle=%v done=%v}\n",
		c.State,
		c.MapTaskSize,
		c.MapTaskIdleSize,
		c.MapTaskDoneSize,
		c.ReduceTaskSize,
		c.ReduceTaskIdleSize,
		c.ReduceTaskDoneSize,
	)

	switch result.TaskKind {
	case TaskKindMap:
		c.handleMapResult(result)
	case TaskKindReduce:
		c.handleReduceResult(result)
	}

	return nil
}

func (c *Coordinator) handleMapResult(result *TaskResult) {
	// The task might have been duplicated due to network issues.
	if c.MapTasks[result.Id].TaskState == TaskStateDone {
		return
	}
	c.updateMapTasksWhenMapStep(result)
	c.updateReduceTasksWhenMapStep(result)
	if c.MapTaskDoneSize == c.MapTaskSize {
		c.State = CoordinatorStateReduce
	}
}

func (c *Coordinator) updateMapTasksWhenMapStep(result *TaskResult) {
	mapTask := c.MapTasks[result.Id]
	mapTask.TaskState = result.TaskState
	c.MapTasks[result.Id] = mapTask
	c.MapTaskDoneSize++
}

func (c *Coordinator) updateReduceTasksWhenMapStep(result *TaskResult) {
	reduceIdAll := result.ParseReduceIdAll()
	for i, reduceId := range reduceIdAll {
		reduceTask, exist := c.ReduceTasks[reduceId]
		if exist {
			reduceTask.CollectTargetFilenameForReduceTask(result.FilenameAll[i])
		} else {
			c.ReduceTaskIdleSize++
			reduceTask = NewReduceTaskViewModelWhenMapTaskDone(reduceId, result.FilenameAll[i], c.ReduceTaskSize)
		}
		c.ReduceTasks[reduceId] = reduceTask
	}
}

func (c *Coordinator) handleReduceResult(result *TaskResult) {
	// The task might have been duplicated due to network issues.
	if c.ReduceTasks[result.Id].TaskState == TaskStateDone {
		return
	}
	reduceTask := c.ReduceTasks[result.Id]
	reduceTask.TaskState = result.TaskState
	c.ReduceTasks[result.Id] = reduceTask
	c.ReduceTaskDoneSize++
	if c.ReduceTaskSize == c.ReduceTaskDoneSize {
		c.done.Store(true)
	}
}

func (c *Coordinator) AcquiredTask(command *AcquireTaskCommand, resp *AcquiredTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("AcquiredTask input: %#v\n", command)
	defer log.Printf("AcquiredTask output: %#v\n", resp)

	var task TaskViewModel
	var err error
	switch c.State {
	case CoordinatorStateMap:
		task, err = c.assignTask(command.ActorId, &c.MapTaskIdleSize, c.MapTasks)
	case CoordinatorStateReduce:
		task, err = c.assignTask(command.ActorId, &c.ReduceTaskIdleSize, c.ReduceTasks)
	}

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

func (c *Coordinator) Connect(_ *ConnectCommand, resp *ConnectResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	actorId := c.NewActorId()
	c.LivedActors[actorId] = true
	*resp = NewConnectResponse(actorId)
	log.Printf("Connect: actorId=%v\n", actorId)
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
	done := c.done.Load()
	if done {
		log.Println("Coordinator task all done !")
	}
	return done
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

	fileQty := len(files)
	tasks := make(map[TaskId]TaskViewModel)
	for i := 0; i < fileQty; i++ {
		id := i
		tasks[id] = NewMapTaskViewModelWhenSetupCoordinator(id, files[i], nReduce)
	}

	c := Coordinator{
		NewActorId:         NewActorId,
		LivedActors:        make(map[ActorId]bool),
		MapTasks:           tasks,
		MapTaskSize:        fileQty,
		MapTaskIdleSize:    fileQty,
		MapTaskDoneSize:    0,
		ReduceTasks:        make(map[TaskId]TaskViewModel, nReduce),
		ReduceTaskSize:     nReduce,
		ReduceTaskIdleSize: 0,
		ReduceTaskDoneSize: 0,
		State:              CoordinatorStateMap,
		done:               atomic.Bool{},
		mu:                 sync.Mutex{},
	}

	log.Printf("Coordinator run: pid=%v nMap=%v nReduce=%v\n", os.Getpid(), fileQty, nReduce)
	c.server()
	return &c
}
