package node

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"time"
)

type Event struct {
	AppID              string
	Allocate           bool
	Timestamp          time.Time
	AllocatedOrRelease *resources.Resource
}

func (e *Event) IsAllocate() bool {
	return e.Allocate
}

func (e *Event) GetAllocatedOrRelease() *resources.Resource {
	return e.AllocatedOrRelease.Clone()
}

func NewAllocatedEvent(appID string, t time.Time, r *resources.Resource) *Event {
	res := r.Clone()
	return &Event{
		AppID:              appID,
		Allocate:           true,
		Timestamp:          t,
		AllocatedOrRelease: res,
	}
}

func NewReleaseEvent(appID string, t time.Time, r *resources.Resource) *Event {
	res := r.Clone()
	return &Event{
		AppID:              appID,
		Allocate:           false,
		Timestamp:          t,
		AllocatedOrRelease: res,
	}
}
