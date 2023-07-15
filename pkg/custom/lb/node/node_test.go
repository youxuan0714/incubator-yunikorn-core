package node

import (
	//"container/heap"
	//fmt
	"github.com/apache/yunikorn-core/pkg/common/resources"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"testing"
	"time"
)

func TestNewNodeResource(t *testing.T) {
	type inputFormat struct {
		max *resources.Resource
		cap *resources.Resource
	}
	tests := []struct {
		caseName string
		input    inputFormat
	}{
		{
			"max and cap",
			inputFormat{
				resources.NewResourceFromMap(map[string]resources.Quantity{sicommon.CPU: resources.Quantity(1600)}),
				resources.NewResourceFromMap(map[string]resources.Quantity{sicommon.CPU: resources.Quantity(2000)}),
			},
		},
		{
			"same resource",
			inputFormat{
				resources.NewResourceFromMap(map[string]resources.Quantity{sicommon.CPU: resources.Quantity(2000)}),
				resources.NewResourceFromMap(map[string]resources.Quantity{sicommon.CPU: resources.Quantity(2000)}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			nr := NewNodeResource(tt.input.max.Clone(), tt.input.cap.Clone())
			if nr.RequestEvents.Len() > 0 || nr.ReleaseEvents.Len() > 0 {
				t.Error("length should be zero")
			}
			if !resources.Equals(nr.Capcity, tt.input.cap) || !resources.Equals(nr.Available, tt.input.max) || !resources.Equals(nr.CurrentAvailable, tt.input.max) {
				t.Error("cap or max avialable is not expect")
			}
		})
	}
}

func TestGetUtilizationWithApp(t *testing.T) {
	cap := resources.NewResourceFromMap(map[string]resources.Quantity{
		sicommon.CPU:    resources.Quantity(200),
		sicommon.Memory: resources.Quantity(200),
	})
	nr := NewNodeResource(cap.Clone(), cap.Clone())
	timestamp := nr.CurrentTime
	utilization := nr.GetUtilization(timestamp, nil)
	expect := resources.NewResourceFromMap(map[string]resources.Quantity{
		sicommon.CPU:    resources.Quantity(0),
		sicommon.Memory: resources.Quantity(0)})
	if !resources.Equals(expect, utilization) {
		t.Errorf("nothing should be zero expect %v, got %v", expect, utilization)
	}

	type inputFormat struct {
		appID     string
		startTime time.Time
		res       *resources.Resource
	}
	allocations := []inputFormat{
		inputFormat{
			appID:     "test-01",
			startTime: timestamp,
			res: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:      resources.Quantity(50),
				sicommon.Memory:   resources.Quantity(25),
				sicommon.Duration: resources.Quantity(25)}),
		},
		inputFormat{
			appID:     "test-02",
			startTime: timestamp,
			res: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:      resources.Quantity(20),
				sicommon.Memory:   resources.Quantity(27),
				sicommon.Duration: resources.Quantity(50)}),
		},
	}
	expected := []*resources.Resource{
		resources.NewResourceFromMap(map[string]resources.Quantity{
			sicommon.CPU:    resources.Quantity(25),
			sicommon.Memory: resources.Quantity(12)}),
		resources.NewResourceFromMap(map[string]resources.Quantity{
			sicommon.CPU:    resources.Quantity(10),
			sicommon.Memory: resources.Quantity(13)}),
	}
	for index, expect := range expected {
		if utilization = nr.GetUtilization(timestamp, allocations[index].res); !resources.Equals(expect, utilization) {
			t.Errorf("Add app utilization expect %v, got %v", expect, utilization)
		}
	}
}

func TestGetUtilization(t *testing.T) {
	cap := resources.NewResourceFromMap(map[string]resources.Quantity{
		sicommon.CPU:    resources.Quantity(100),
		sicommon.Memory: resources.Quantity(100),
	})
	nr := NewNodeResource(cap.Clone(), cap.Clone())
	timestamp := nr.CurrentTime
	type inputFormat struct {
		appID     string
		startTime time.Time
		res       *resources.Resource
	}
	allocations := []inputFormat{
		inputFormat{
			appID:     "test-01",
			startTime: timestamp,
			res: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:      resources.Quantity(50),
				sicommon.Memory:   resources.Quantity(25),
				sicommon.Duration: resources.Quantity(25)}),
		},
		inputFormat{
			appID:     "test-02",
			startTime: timestamp,
			res: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:      resources.Quantity(20),
				sicommon.Memory:   resources.Quantity(27),
				sicommon.Duration: resources.Quantity(50)}),
		},
	}
	for _, allocation := range allocations {
		nr.Allocate(allocation.appID, allocation.startTime, allocation.res)
	}
	expected := []inputFormat{
		inputFormat{
			appID:     "t=5",
			startTime: timestamp.Add(time.Second * 5),
			res: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:    resources.Quantity(70),
				sicommon.Memory: resources.Quantity(52)}),
		},
		inputFormat{
			appID:     "t=25",
			startTime: timestamp.Add(time.Second * 25),
			res: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:    resources.Quantity(20),
				sicommon.Memory: resources.Quantity(27)}),
		},
		inputFormat{
			appID:     "5=50",
			startTime: timestamp.Add(time.Second * 50),
			res: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:    resources.Quantity(0),
				sicommon.Memory: resources.Quantity(0)}),
		},
	}
	for index, expect := range expected {
		utilization := nr.GetUtilization(expect.startTime, nil)
		if !resources.Equals(expect.res.Clone(), utilization) {
			t.Errorf("time %d, expect %v, got %v", index, expect.res.Clone(), utilization)
		}
	}
}
