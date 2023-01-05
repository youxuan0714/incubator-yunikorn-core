package node

import (
	"container/heap"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"
	"time"
)

type NodeResource struct {
	RequestEvents *Events
	Available     *resources.Resource
	Capcity       *resources.Resource
	MaxAvialable  *resources.Resource
	CurrentTime   time.Time
}

func NewNodeResource(Available *resources.Resource, cap *resources.Resource) *NodeResource {
	s := Available.Clone()
	delete(s.Resources, sicommon.Duration)
	return &NodeResource{
		RequestEvents: NewEvents(),
		Available:     s.Clone(),
		MaxAvialable:  s.Clone(),
		Capcity:       cap.Clone(),
		CurrentTime:   time.Now(),
	}
}

func (n *NodeResource) Allocate(appID string, allocateTime time.Time, req *resources.Resource) {
	releaseTime := allocateTime.Add(time.Second * time.Duration(req.Resources[sicommon.Duration]))
	s := req.Clone()
	delete(s.Resources, sicommon.Duration)
	heap.Push(n.RequestEvents, NewReleaseEvent(appID, releaseTime, s.Clone()))
	heap.Push(n.RequestEvents, NewAllocatedEvent(appID, allocateTime, s.Clone()))
	log.Logger().Info("Current events heap", zap.Int("length", n.RequestEvents.Len()))
}

func (n *NodeResource) GetUtilization(timeStamp time.Time, request *resources.Resource) (utilization *resources.Resource) {
	bk := make([]*Event, 0)
	available := n.Available.Clone()
	log.Logger().Info("calculate utilization of avaialble", zap.String("avialble", available.String()))
	for n.RequestEvents.Len() > 0 {
		event := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, event)
		// when there is t=5 submit, that means the events before t=5 should be handle and calucalte in to temperal resources
		if !event.Timestamp.After(timeStamp) {
			AllocatedOrRelease := event.AllocatedOrRelease.Clone()
			if event.Allocate {
				log.Logger().Info("Allocate", zap.Any("timestamp", event.Timestamp), zap.String("allocate", AllocatedOrRelease.String()))
				available = resources.Sub(available, AllocatedOrRelease)
			} else {
				log.Logger().Info("Release", zap.Any("timestamp", event.Timestamp), zap.String("release", AllocatedOrRelease.String()))
				available = resources.Add(available, AllocatedOrRelease)
			}
		} else {
			break
		}
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}

	total := n.Capcity.Clone() //cpu and memory
	log.Logger().Info("calculate utilization", zap.String("cap", total.String()), zap.String("avialble", available.String()))
	resourceAllocated := resources.Sub(total, available)
	if request != nil {
		tmp := request.Clone()
		if _, ok := tmp.Resources[sicommon.Duration]; ok {
			delete(tmp.Resources, sicommon.Duration)
		}
		// cpu and memory, without duration
		resourceAllocated = resources.Add(resourceAllocated, tmp)
	}

	return &resources.Resource{Resources: resources.CalculateAbsUsedCapacity(total, resourceAllocated).Resources}
}

func (n *NodeResource) WhenCanStart(submitTime time.Time, req *resources.Resource) (bool, time.Time) {
	log.Logger().Info("find when can start", zap.Any("submit", submitTime), zap.String("request", req.String()))
	applicationReq := req.Clone()
	delete(applicationReq.Resources, sicommon.Duration)

	if enoughCapicity := resources.StrictlyGreaterThanOrEquals(n.MaxAvialable, applicationReq); !enoughCapicity {
		log.Logger().Info("not enough cap", zap.String("max avial", n.MaxAvialable.String()), zap.String("app", applicationReq.String()))
		return enoughCapicity, submitTime
	}

	// clear outdated event and update
	var startTime time.Time
	bk := make([]*Event, 0)
	available := n.Available.Clone()
	for n.RequestEvents.Len() > 0 {
		log.Logger().Info("events length", zap.Int("events", n.RequestEvents.Len()))
		event := heap.Pop(n.RequestEvents).(*Event)
		if event.Timestamp.Equal(submitTime) || event.Timestamp.Before(submitTime) {
			bk = append(bk, event)
			if event.Allocate {
				available = resources.Sub(available, event.AllocatedOrRelease)
				log.Logger().Info("skip: when could start Sub", zap.Int("events", n.RequestEvents.Len()), zap.String("avialable", available.String()))
			} else {
				available = resources.Add(available, event.AllocatedOrRelease)
				log.Logger().Info("skip: When could start Add", zap.Int("events", n.RequestEvents.Len()), zap.String("avialable", available.String()))
			}
		} else {
			heap.Push(n.RequestEvents, event)
			break
		}
	}

	startTime = submitTime
	for !resources.StrictlyGreaterThanOrEquals(available, applicationReq) {
		log.Logger().Info("events length", zap.Int("events", n.RequestEvents.Len()))
		if n.RequestEvents.Len() > 0 {
			event := heap.Pop(n.RequestEvents).(*Event)
			bk = append(bk, event)
			startTime = event.Timestamp
			if event.Allocate {
				available = resources.Sub(available, event.AllocatedOrRelease)
				log.Logger().Info("When could start Sub", zap.Int("events", n.RequestEvents.Len()), zap.String("avialable", available.String()))
			} else {
				available = resources.Add(available, event.AllocatedOrRelease)
				log.Logger().Info("When could start Add", zap.Int("events", n.RequestEvents.Len()), zap.String("avialable", available.String()))
			}
		} else if n.RequestEvents.Len() == 0 {
			for _, element := range bk {
				heap.Push(n.RequestEvents, element)
			}
			log.Logger().Info("not enough cap", zap.String("max avial", n.MaxAvialable.String()), zap.String("avail", available.String()))
			return false, submitTime
		}
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}

	log.Logger().Info("Can start at", zap.Any("startTime", startTime))
	return true, startTime
}
