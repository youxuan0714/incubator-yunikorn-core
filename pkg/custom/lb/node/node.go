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
	CurrentTime   time.Time
}

func NewNodeResource(Available *resources.Resource, cap *resources.Resource) *NodeResource {
	s := Available.Clone()
	delete(s.Resources, sicommon.Duration)
	return &NodeResource{
		RequestEvents: NewEvents(),
		Available:     s.Clone(),
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
}

func (n *NodeResource) GetUtilization(timeStamp time.Time, request *resources.Resource) (utilization *resources.Resource) {
	bk := make([]*Event, 0)
	available := n.Available.Clone()
	log.Logger().Info("calculate utilization of avaialble", zap.String("avialble", available.String()))
	for n.RequestEvents.Len() > 0 {
		tmp := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, tmp)
		// when there is t=5 submit, that means the events before t=5 should be handle and calucalte in to temperal resources
		if timeStamp.Before(tmp.Timestamp) {
			break
		} else {
			AllocatedOrRelease := tmp.AllocatedOrRelease.Clone()
			if tmp.Allocate {
				log.Logger().Info("Allocate", zap.String("allocate", AllocatedOrRelease.String()))
				available = resources.Sub(available, AllocatedOrRelease)
			} else {
				log.Logger().Info("Release", zap.String("release", AllocatedOrRelease.String()))
				available = resources.Add(available, AllocatedOrRelease)
			}
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

func (n *NodeResource) WhenCanStart(submitTime time.Time, req *resources.Resource) (enoughCapicity bool, startTime time.Time) {
	applicationReq := req.Clone()
	if _, ok := applicationReq.Resources[sicommon.Duration]; ok {
		delete(applicationReq.Resources, sicommon.Duration)
	}
	startTime = n.CurrentTime
	if enoughCapicity = resources.StrictlyGreaterThanOrEquals(n.Capcity, applicationReq); !enoughCapicity {
		return
	} else {
		enoughCapicity = true
	}

	// clear outdated event and update
	_ = n.ClearEventsBaseOnSubmittedTime(submitTime)

	bk := make([]*Event, 0)
	available := n.Available.Clone()
	startTime = n.CurrentTime
	for n.RequestEvents.Len() > 0 && !resources.StrictlyGreaterThanOrEquals(available, applicationReq) {
		tmp := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, tmp)
		if tmp.Allocate {
			available = resources.Sub(available, tmp.AllocatedOrRelease)
		} else {
			available = resources.Add(available, tmp.AllocatedOrRelease)
		}
		startTime = tmp.Timestamp
	}

	if startTime.Before(submitTime) {
		startTime = submitTime
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}
	return
}

func (n *NodeResource) ClearEventsBaseOnSubmittedTime(submitTime time.Time) *resources.Resource {
	available := n.Available.Clone()
	for n.RequestEvents.Len() > 0 {
		tmp := heap.Pop(n.RequestEvents).(*Event)
		if !tmp.Timestamp.After(submitTime) {
			if tmp.Allocate {
				available = resources.Sub(available, tmp.AllocatedOrRelease)
			} else {
				available = resources.Add(available, tmp.AllocatedOrRelease)
			}
			n.CurrentTime = tmp.Timestamp
		} else {
			heap.Push(n.RequestEvents, tmp)
			break
		}
	}
	n.Available = available
	return n.Available.Clone()
}
