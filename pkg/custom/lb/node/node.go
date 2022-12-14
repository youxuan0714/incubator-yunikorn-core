package node

import (
	"container/heap"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"time"
)

type NodeResource struct {
	RequestEvents *Events
	Available     *resources.Resource
	Capcity       *resources.Resource
	CurrentTime   time.Time
}

func NewNodeResource(Available *resources.Resource) *NodeResource {
	return &NodeResource{
		RequestEvents: NewEvents(),
		Available:     Available.Clone(),
		Capcity:       Available.Clone(),
		CurrentTime:   time.Now(),
	}
}

func (n *NodeResource) Allocate(appID string, allocateTime time.Time, req *resources.Resource) {
	releaseTime := allocateTime.Add(time.Second * time.Duration(req.Resources[sicommon.Duration]))
	heap.Push(n.RequestEvents, NewReleaseEvent(appID, releaseTime, req))
	heap.Push(n.RequestEvents, NewAllocatedEvent(appID, allocateTime, req))
}

func (n *NodeResource) GetUtilization(timeStamp time.Time, request *resources.Resource) (utilization *resources.Resource) {
	bk := make([]*Event, 0)
	available := n.Available.Clone()
	for n.RequestEvents.Len() > 0 {
		tmp := heap.Pop(n.RequestEvents).(*Event)
		bk = append(bk, tmp)
		if timeStamp.Before(tmp.Timestamp) {
			break
		} else {
			if tmp.Allocate {
				available = resources.Sub(available, tmp.AllocatedOrRelease)
			} else {
				available = resources.Add(available, tmp.AllocatedOrRelease)
			}
		}
	}

	for _, element := range bk {
		heap.Push(n.RequestEvents, element)
	}

	total := n.Capcity
	resourceAllocated := resources.Sub(n.Capcity, available)
	if request != nil {
		tmp := request.Clone()
		if _, ok := tmp.Resources[sicommon.Duration]; ok {
			delete(tmp.Resources, sicommon.Duration)
		}
		resourceAllocated = resources.Sub(resourceAllocated, tmp)
	}
	utilizedResource := make(map[string]resources.Quantity)

	for name := range resourceAllocated.Resources {
		if total.Resources[name] > 0 {
			utilizedResource[name] = resources.CalculateAbsUsedCapacity(total, resourceAllocated).Resources[name]
		}
	}
	return &resources.Resource{Resources: utilizedResource}
}

func (n *NodeResource) WhenCanStart(submitTime time.Time, req *resources.Resource) (ExcessCapicity bool, startTime time.Time) {
	startTime = n.CurrentTime
	if enoughCapicity := resources.StrictlyGreaterThanOrEquals(n.Capcity, req); !enoughCapicity {
		return
	} else {
		enoughCapicity = true
	}

	// clear outdated event and update
	n.ClearEventsBaseOnSubmittedTime(submitTime)

	bk := make([]*Event, 0)
	available := n.Available.Clone()
	startTime = n.CurrentTime
	for n.RequestEvents.Len() > 0 && !resources.StrictlyGreaterThanOrEquals(available, req) {
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

func (n *NodeResource) ClearEventsBaseOnSubmittedTime(submitTime time.Time) {
	available := n.Available.Clone()
	for n.RequestEvents.Len() > 0 {
		tmp := heap.Pop(n.RequestEvents).(*Event)
		if tmp.Timestamp.Before(submitTime) || tmp.Timestamp.Equal(submitTime) {
			if tmp.Allocate {
				available = resources.Sub(available, tmp.AllocatedOrRelease)
			} else {
				available = resources.Add(available, tmp.AllocatedOrRelease)
			}
			n.CurrentTime = tmp.Timestamp
			continue
		}
		heap.Push(n.RequestEvents, tmp)
		break
	}
}
