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
	NewActorId func() ActorId
	LivedActor map[ActorId]bool

	MapTasks    map[TaskId]TaskViewModel
	MapTaskSize int

	ReduceTasks    map[TaskId]TaskViewModel
	ReduceTaskSize int

	mu sync.Mutex
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

	fileQty := len(files)
	tasks := make(map[TaskId]TaskViewModel)
	for i := 0; i < fileQty; i++ {
		id := i
		tasks[id] = NewTaskViewModelWhenSetupCoordinator(id, files[i], nReduce)
	}

	c := Coordinator{
		NewActorId:     NewActorId,
		LivedActor:     make(map[ActorId]bool),
		MapTasks:       tasks,
		MapTaskSize:    fileQty,
		ReduceTasks:    make(map[TaskId]TaskViewModel, nReduce),
		ReduceTaskSize: nReduce,
		mu:             sync.Mutex{},
	}

	log.Printf("coordinator run: pid=%v\n", os.Getpid())
	c.server()
	return &c
}
