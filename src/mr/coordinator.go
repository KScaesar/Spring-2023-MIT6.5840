package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	NewTaskId  func() TaskId
	NewActorId func() ActorId

	WaitedTasks     map[TaskId]TaskViewModel
	WaitedTaskIdAll []TaskId
	WaitedTaskSize  int

	AssignedTasks map[ActorId]TaskViewModel
	IdleTaskIdAll []ActorId
	IdleTaskSize  int

	nReduce int
	mu      sync.Mutex
}

func (c *Coordinator) RegisterActor(_ *RegisterActorCommand, resp *RegisteredActorResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task TaskViewModel
	actorId := c.NewActorId()
	if c.WaitedTaskSize == 0 {
		task = c.assignIdleTask(actorId)
	} else {
		task = c.assignBusyTask(actorId)
	}
	*resp = NewCreatedActorResponse(actorId, &task)

	log.Printf("RegisterActor: actorId=%v assignBusyTask=%v\n", actorId, task)
	return nil
}

func (c *Coordinator) assignIdleTask(actorId ActorId) TaskViewModel {
	c.IdleTaskSize++
	c.IdleTaskIdAll = append(c.IdleTaskIdAll, actorId)
	return NewIdleTaskViewModel(c.NewTaskId())
}

func (c *Coordinator) assignBusyTask(actorId ActorId) TaskViewModel {
	task := c.popWaitedTasks()
	c.AssignedTasks[actorId] = task
	return task
}

func (c *Coordinator) popWaitedTasks() TaskViewModel {
	lastIndex := c.WaitedTaskSize - 1
	taskId := c.WaitedTaskIdAll[lastIndex]
	c.WaitedTaskSize--
	c.WaitedTaskIdAll = c.WaitedTaskIdAll[:c.WaitedTaskSize]
	task := c.WaitedTasks[taskId]
	delete(c.WaitedTasks, taskId)
	return task
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
	var taskId TaskId
	NewTaskId := func() TaskId {
		taskId++
		return taskId
	}

	var actorId ActorId
	NewActorId := func() ActorId {
		actorId++
		return actorId
	}

	fileQty := len(files)
	tasks := make(map[TaskId]TaskViewModel)
	taskIdAll := make([]TaskId, 0, fileQty)
	for i := 0; i < fileQty; i++ {
		id := NewTaskId()
		tasks[id] = NewTaskViewModelWhenSetupCoordinator(id, files[i])
		taskIdAll = append(taskIdAll, id)
	}

	c := Coordinator{
		NewTaskId:       NewTaskId,
		NewActorId:      NewActorId,
		WaitedTasks:     tasks,
		WaitedTaskIdAll: taskIdAll,
		WaitedTaskSize:  fileQty,
		AssignedTasks:   make(map[ActorId]TaskViewModel),
		IdleTaskIdAll:   make([]ActorId, 0),
		IdleTaskSize:    0,
		nReduce:         nReduce,
		mu:              sync.Mutex{},
	}

	log.Printf("coordinator run: pid=%v\n", os.Getpid())
	c.server()
	return &c
}
