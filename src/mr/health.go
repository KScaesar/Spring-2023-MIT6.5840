package mr

import (
	"log"
	"sync"
	"time"
)

func NewHealthChecker(timeout time.Duration, logger *log.Logger) *HealthChecker {
	return &HealthChecker{
		timeoutLimit: timeout,
		logger:       logger,
	}
}

type HealthChecker struct {
	livedConnections sync.Map // map[Identifier]chan Struct{}

	// using notifyAction to determine whether connection have ever joined before.
	notifyAction sync.Map // map[Identifier]func(id T)

	timeoutLimit time.Duration
	logger       *log.Logger
}

func (hc *HealthChecker) JoinConnection(id ActorId, notifyActionWhenConnectionDead func(id ActorId)) {
	hc.setupPingEnvironment(id, notifyActionWhenConnectionDead)
	log.Printf("Connect: identifiier=%#v\n", id)
}

func (hc *HealthChecker) setupPingEnvironment(id ActorId, notify func(id ActorId)) {
	timeout := time.NewTimer(hc.timeoutLimit)
	healthChannel := make(chan struct{})
	hc.livedConnections.Store(id, healthChannel)
	hc.notifyAction.Store(id, notify)

	go func() {
		for {
			select {
			case <-timeout.C:
				timeout.Stop()
				hc.livedConnections.Delete(id)
				notify(id)
				hc.logger.Printf("connection[%v] maybe dead\n", id)
				return

			case <-healthChannel:
				hc.logger.Printf("connection[%v] still live\n", id)
				timeout.Reset(hc.timeoutLimit)
			}
		}
	}()
}

func (hc *HealthChecker) Ping(id ActorId) {
	health, isLive := hc.getHealthChannel(id)
	if isLive {
		go func() {
			after := time.After(30 * time.Second) // connection maybe dead
			select {
			case <-after:
				return
			case health <- struct{}{}:
			}
		}()
		return
	}

	hc.reJoinConnectionForHandleNetworkIssue(id)
}

func (hc *HealthChecker) reJoinConnectionForHandleNetworkIssue(id ActorId) {
	notifyAction, isJoin := hc.getNotifyAction(id)
	if !isJoin {
		hc.logger.Fatalln("Must call JoinConnection before calling the Ping method.")
	}
	hc.JoinConnection(id, notifyAction)
}

func (hc *HealthChecker) getHealthChannel(id ActorId) (ch chan struct{}, isLive bool) {
	value, isLive := hc.livedConnections.Load(id)
	return value.(chan struct{}), isLive
}

func (hc *HealthChecker) getNotifyAction(id ActorId) (ch func(id ActorId), isJoin bool) {
	value, isJoin := hc.notifyAction.Load(id)
	return value.(func(id ActorId)), isJoin
}
