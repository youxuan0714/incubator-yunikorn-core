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
	log.Logger().Info("expect", zap.String("allocate", n.GetUtilization(allocateTime, nil).String()), zap.String("release", n.GetUtilization(releaseTime, req.Clone()).String()))
}

func (n *NodeResource) GetUtilization(timeStamp time.Time, request *resources.Resource) (utilization *resources.Resource) {
	bk := make([]*Event, 0)
	available := n.Available.Clone()
	log.Logger().Info("Get utilization", zap.String("initial avialble", available.String()))
	if request != nil {
		log.Logger().Info("get utilization with request")
	}
	for n.RequestEvents.Len() > 0 {
		log.Logger().Info("events", zap.Int("length", n.RequestEvents.Len()))
		event := heap.Pop(n.RequestEvents).(*Event)
		// when there is t=5 submit, that means the events before t=5 should be handle and calucalte in to temperal resources
		if event.Timestamp.Equal(timeStamp) || event.Timestamp.Before(timeStamp) {
			bk = append(bk, event)
			AllocatedOrRelease := event.AllocatedOrRelease.Clone()
			if event.Allocate {
				available = resources.Sub(available, AllocatedOrRelease)
				log.Logger().Info("Allocate", zap.Any("timestamp", event.Timestamp), zap.String("avail", available.String()), zap.String("allocate", AllocatedOrRelease.String()))
			} else {
				available = resources.Add(available, AllocatedOrRelease)
				log.Logger().Info("Release", zap.Any("timestamp", event.Timestamp), zap.String("avail", available.String()), zap.String("release", AllocatedOrRelease.String()))
			}
		} else {
			heap.Push(n.RequestEvents, event)
			for n.RequestEvents.Len() > 0 {
				tmp := heap.Pop(n.RequestEvents).(*Event)
				bk = append(bk, tmp)
				if tmp.Allocate {
					log.Logger().Info("other allocate utilization events", zap.Any("timestamp", tmp.Timestamp), zap.String("resource", tmp.AllocatedOrRelease.String()))
				} else {
					log.Logger().Info("other release utilization events", zap.Any("timestamp", tmp.Timestamp), zap.String("resource", tmp.AllocatedOrRelease.String()))
				}
			}
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
	startTime := submitTime
	bk := make([]*Event, 0)
	available := n.Available.Clone()
	for n.RequestEvents.Len() > 0 {
		log.Logger().Info("events length", zap.Int("events", n.RequestEvents.Len()))
		event := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, event)
		if event.Timestamp.Before(submitTime) || event.Timestamp.Equal(submitTime) {
			if event.Allocate {
				available = resources.Sub(available, event.AllocatedOrRelease)
				log.Logger().Info("skip: when could start Sub", zap.Any("time", event.Timestamp), zap.String("avialable", available.String()), zap.String("event", event.AllocatedOrRelease.String()))
			} else {
				available = resources.Add(available, event.AllocatedOrRelease)
				log.Logger().Info("skip: When could start Add", zap.Any("time", event.Timestamp), zap.String("avialable", available.String()), zap.String("event", event.AllocatedOrRelease.String()))
			}
		} else {
			heap.Push(n.RequestEvents, event)
			break
		}
	}

	for !resources.StrictlyGreaterThanOrEquals(available, applicationReq) && n.RequestEvents.Len() > 0 {
		log.Logger().Info("events length", zap.Int("events", n.RequestEvents.Len()))
		event := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, event)
		startTime = event.Timestamp
		if event.Allocate {
			available = resources.Sub(available, event.AllocatedOrRelease)
			log.Logger().Info("When could start Sub", zap.Any("time", event.Timestamp), zap.String("avialable", available.String()), zap.String("event", event.AllocatedOrRelease.String()))
		} else {
			available = resources.Add(available, event.AllocatedOrRelease)
			log.Logger().Info("When could start Add", zap.Any("time", event.Timestamp), zap.String("avialable", available.String()), zap.String("event", event.AllocatedOrRelease.String()))
		}
	}

	for n.RequestEvents.Len() > 0 {
		event := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, event)
		if event.Allocate {
			log.Logger().Info("When could start other allocate", zap.Any("time", event.Timestamp), zap.String("avialable", available.String()), zap.String("event", event.AllocatedOrRelease.String()))
		} else {
			log.Logger().Info("When could start other release", zap.Any("time", event.Timestamp), zap.String("avialable", available.String()), zap.String("event", event.AllocatedOrRelease.String()))
		}
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}

	log.Logger().Info("Can start at", zap.Any("startTime", startTime), zap.String("availble res", available.String()), zap.String("req", applicationReq.String()))
	return true, startTime
}
