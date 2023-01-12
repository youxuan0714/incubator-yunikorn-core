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
	request := removeDurationInApp(req)
	heap.Push(n.RequestEvents, NewReleaseEvent(appID, releaseTime, request.Clone()))
	heap.Push(n.RequestEvents, NewAllocatedEvent(appID, allocateTime, request.Clone()))
	log.Logger().Info("Current events heap", zap.Int("length", n.RequestEvents.Len()))
	log.Logger().Info("expect", zap.String("allocate", n.GetUtilization(allocateTime, nil).String()), zap.String("release", n.GetUtilization(releaseTime, req.Clone()).String()))
}

func (n *NodeResource) GetUtilization(timeStamp time.Time, request *resources.Resource) (utilization *resources.Resource) {
	if request != nil {
		log.Logger().Info("get utilization with request")
	}

	available := n.getAvialableAtTimeT(timeStamp)
	total := n.Capcity.Clone() //cpu and memory
	log.Logger().Info("calculate utilization", zap.Any("timestamp", timeStamp), zap.String("cap", total.String()), zap.String("avialble", available.String()))
	allocated := resources.Sub(total, available)
	if request != nil {
		tmp := removeDurationInApp(request)
		allocated = resources.Add(allocated, tmp)
	}

	return &resources.Resource{Resources: resources.CalculateAbsUsedCapacity(total, allocated).Resources}
}

func (n *NodeResource) getAvialableAtTimeT(timeStamp time.Time) *resources.Resource {
	bk := make([]*Event, 0)
	result := n.Available.Clone()
	for n.RequestEvents.Len() > 0 {
		// when there is t=5 submit, that means the events before t=5 should be handle and calucalte in to temperal resources
		event := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, event)
		if event.Timestamp.Equal(timeStamp) || event.Timestamp.Before(timeStamp) {
			result = handleEvent(event, result)
		} else {
			printEventInfo(event)
		}
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}
	return result
}

func (n *NodeResource) WhenCanStart(submitTime time.Time, req *resources.Resource) (bool, time.Time) {
	log.Logger().Info("find when can start", zap.Any("submit", submitTime), zap.String("request", req.String()))
	applicationReq := removeDurationInApp(req)

	if enoughCapicity := resources.StrictlyGreaterThanOrEquals(n.MaxAvialable, applicationReq); !enoughCapicity {
		log.Logger().Info("not enough cap", zap.String("max avial", n.MaxAvialable.String()), zap.String("app", applicationReq.String()))
		return enoughCapicity, submitTime
	}

	// clear outdated event and update
	bk := make([]*Event, 0)
	startTime := submitTime
	available := n.Available.Clone()
	for n.RequestEvents.Len() > 0 {
		event := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, event)
		if event.Timestamp.Before(submitTime) || event.Timestamp.Equal(submitTime) {
			available = handleEvent(event, available)
		} else {
			heap.Push(n.RequestEvents, event)
			break
		}
	}

	for !resources.StrictlyGreaterThanOrEquals(available, applicationReq) {
		var timestamp time.Time
		for first := true; n.RequestEvents.Len() > 0; {
			event := heap.Pop(n.RequestEvents).(*Event)
			if first {
				timestamp = event.Timestamp
				first = false
			} else if !event.Timestamp.Equal(timestamp) {
				heap.Push(n.RequestEvents, event)
				break
			}
			bk = append(bk, event)
			available = handleEvent(event, available)
			startTime = event.Timestamp
		}
	}

	for n.RequestEvents.Len() > 0 {
		event := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, event)
		printEventInfo(event)
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}

	log.Logger().Info("Can start at", zap.Any("startTime", startTime), zap.String("availble res", available.String()), zap.String("req", applicationReq.String()))
	return true, startTime
}

func handleEvent(event *Event, available *resources.Resource) *resources.Resource {
	result := available.Clone()
	AllocatedOrRelease := event.GetAllocatedOrRelease()
	if event.IsAllocate() {
		result = resources.Sub(result, AllocatedOrRelease)
		log.Logger().Info("Allocate", zap.Any("timestamp", event.Timestamp), zap.String("avail", result.String()), zap.String("allocate", AllocatedOrRelease.String()))
	} else {
		result = resources.Add(result, AllocatedOrRelease)
		log.Logger().Info("Release", zap.Any("timestamp", event.Timestamp), zap.String("avail", result.String()), zap.String("release", AllocatedOrRelease.String()))
	}
	return result
}

func printEventInfo(event *Event) {
	if AllocatedOrRelease := event.GetAllocatedOrRelease(); event.IsAllocate() {
		log.Logger().Info("last allocate", zap.Any("time", event.Timestamp), zap.String("event", AllocatedOrRelease.String()))
	} else {
		log.Logger().Info("last release", zap.Any("time", event.Timestamp), zap.String("event", AllocatedOrRelease.String()))
	}
}

func removeDurationInApp(req *resources.Resource) *resources.Resource {
	tmp := req.Clone()
	delete(tmp.Resources, sicommon.Duration)
	return tmp
}
