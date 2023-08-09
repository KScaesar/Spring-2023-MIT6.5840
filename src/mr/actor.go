package mr

import (
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
	Task      Task
	mapper    func(string, string) []KeyValue
	reducer   func(string, []string) string
}

func (a *Actor) Run() {
	log.Printf("actor run: actor_id=%v, processId=%v\n", a.Id, a.ProcessId)
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			log.Println("ticker")
		}
	}
}
