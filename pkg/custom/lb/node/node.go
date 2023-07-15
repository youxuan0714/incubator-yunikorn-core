package node

import (
	"container/heap"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"
)

type NodeResource struct {
	RequestEvents    *Events
	ReleaseEvents    *Events
	Available        *resources.Resource
	Capcity          *resources.Resource
	MaxAvialable     *resources.Resource
	CurrentAvailable *resources.Resource
	CurrentTime      time.Time
}

func NewNodeResource(Available *resources.Resource, cap *resources.Resource) *NodeResource {
	s := Available.Clone()
	delete(s.Resources, sicommon.Duration)
	return &NodeResource{
		RequestEvents:    NewEvents(),
		ReleaseEvents:    NewEvents(),
		Available:        s.Clone(),
		MaxAvialable:     s.Clone(),
		Capcity:          cap.Clone(),
		CurrentAvailable: s.Clone(),
		CurrentTime:      time.Now(),
	}
}

func (n *NodeResource) Allocate(appID string, allocateTime time.Time, req *resources.Resource) {
	releaseTime := allocateTime.Add(time.Second * time.Duration(req.Resources[sicommon.Duration]))
	request := removeDurationInApp(req)
	releaseEvent := NewReleaseEvent(appID, releaseTime, request.Clone())
	allocateEvent := NewAllocatedEvent(appID, allocateTime, request.Clone())
	heap.Push(n.RequestEvents, releaseEvent)
	heap.Push(n.RequestEvents, allocateEvent)

	n.UpdateLastestRequest(req)
	heap.Push(n.ReleaseEvents, releaseEvent)
	// log.Logger().Info("Current events heap", zap.Int("length", n.RequestEvents.Len()))
	// log.Logger().Info("expect", zap.String("allocate", n.GetUtilization(allocateTime, nil).String()), zap.String("release", n.GetUtilization(releaseTime, req.Clone()).String()))
}

func (n *NodeResource) GetUtilization(timeStamp time.Time, request *resources.Resource) (utilization *resources.Resource) {
	/*
		if request != nil {
			log.Logger().Info("get utilization with request")
		}
	*/

	available := n.getAvialableAtTimeT(timeStamp)
	total := n.Capcity.Clone()
	// log.Logger().Info("calculate utilization", zap.Any("timestamp", timeStamp), zap.String("cap", total.String()), zap.String("avialble", available.String()))
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
			break
			//printEventInfo(event)
		}
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}
	return result
}

func (n *NodeResource) UpdateLastestRequest(req *resources.Resource) {
	applicationReq := removeDurationInApp(req.Clone())
	available := n.CurrentAvailable.Clone()
	for n.ReleaseEvents.Len() > 0 && !resources.StrictlyGreaterThanOrEquals(available, applicationReq) {
		event := heap.Pop(n.ReleaseEvents).(*Event)
		available = handleEvent(event, available)
		n.CurrentTime = event.Timestamp
	}
	n.CurrentAvailable = resources.Sub(available, applicationReq)
	return
}

func handleEvent(event *Event, available *resources.Resource) *resources.Resource {
	result := available.Clone()
	AllocatedOrRelease := event.GetAllocatedOrRelease()
	if event.IsAllocate() {
		result = resources.Sub(result, AllocatedOrRelease)
	} else {
		result = resources.Add(result, AllocatedOrRelease)
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

type SimpleNode struct {
	Available *resources.Resource
	Capcity   *resources.Resource
	Usage     *resources.Resource
}

func NewSimpleNode(a *resources.Resource, c *resources.Resource, u *resources.Resource) *SimpleNode {
	return &SimpleNode{
		Available: a,
		Capcity:   c,
		Usage:     u,
	}
}
