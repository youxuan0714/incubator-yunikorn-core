package node

import (
	"container/heap"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"testing"
	"time"
)

func TestNewNodeResource(t *testing.T) {

	cap := resources.NewResource()
	cap.Resources[sicommon.CPU] = resources.Quantity(100)
	cap.Resources[sicommon.Memory] = resources.Quantity(100)
	nr := NewNodeResource(cap)
	if nr.RequestEvents.Len() > 0 {
		t.Error("length should be zero")
	}
	if tmp := time.Now(); tmp.Before(nr.CurrentTime) {
		t.Errorf("expect %v should after the got %v", tmp, nr.CurrentTime)
	}

	if !resources.Equals(nr.Capcity, cap) {
		t.Error("Capcity should be same ")
	}

	if !resources.Equals(nr.Available, cap) {
		t.Error("Capcity and avaiable should be same ")
	}
}

func TestAllocate(t *testing.T) {
	cap := resources.NewResource()
	cap.Resources[sicommon.CPU] = resources.Quantity(100)
	cap.Resources[sicommon.Memory] = resources.Quantity(100)
	nr := NewNodeResource(cap)
	timestamp := nr.CurrentTime

	type inputFormat struct {
		appID     string
		startTime time.Time
		res       *resources.Resource
	}

	type outputFormat struct {
		eventNumber int
		allocate    []bool
		information []inputFormat
	}

	tests := []struct {
		caseName string
		input    inputFormat
		expected outputFormat
	}{
		{
			caseName: "allocate basic function",
			input: inputFormat{
				appID:     "test-01",
				startTime: timestamp,
				res: resources.NewResourceFromMap(map[string]resources.Quantity{
					sicommon.CPU:      resources.Quantity(50),
					sicommon.Memory:   resources.Quantity(25),
					sicommon.Duration: resources.Quantity(25)}),
			},
			expected: outputFormat{
				eventNumber: 2,
				allocate:    []bool{true, false},
				information: []inputFormat{
					inputFormat{
						appID:     "test-01",
						startTime: timestamp,
						res: resources.NewResourceFromMap(map[string]resources.Quantity{
							sicommon.CPU:    resources.Quantity(50),
							sicommon.Memory: resources.Quantity(25)}),
					},
					inputFormat{
						appID:     "test-01",
						startTime: timestamp.Add(time.Second * time.Duration(25)),
						res: resources.NewResourceFromMap(map[string]resources.Quantity{
							sicommon.CPU:    resources.Quantity(50),
							sicommon.Memory: resources.Quantity(25)}),
					},
				},
			},
		},
		{
			caseName: "Next allocation",
			input: inputFormat{
				appID:     "test-02",
				startTime: timestamp,
				res: resources.NewResourceFromMap(map[string]resources.Quantity{
					sicommon.CPU:      resources.Quantity(50),
					sicommon.Memory:   resources.Quantity(25),
					sicommon.Duration: resources.Quantity(50)}),
			},
			expected: outputFormat{
				eventNumber: 4,
				allocate:    []bool{true, true, false, false},
				information: []inputFormat{
					inputFormat{
						appID:     "test-01",
						startTime: timestamp,
						res: resources.NewResourceFromMap(map[string]resources.Quantity{
							sicommon.CPU:    resources.Quantity(50),
							sicommon.Memory: resources.Quantity(25)}),
					},
					inputFormat{
						appID:     "test-02",
						startTime: timestamp,
						res: resources.NewResourceFromMap(map[string]resources.Quantity{
							sicommon.CPU:    resources.Quantity(50),
							sicommon.Memory: resources.Quantity(25)}),
					},
					inputFormat{
						appID:     "test-01",
						startTime: timestamp.Add(time.Second * time.Duration(25)),
						res: resources.NewResourceFromMap(map[string]resources.Quantity{
							sicommon.CPU:    resources.Quantity(50),
							sicommon.Memory: resources.Quantity(25)}),
					},
					inputFormat{
						appID:     "test-02",
						startTime: timestamp.Add(time.Second * time.Duration(50)),
						res: resources.NewResourceFromMap(map[string]resources.Quantity{
							sicommon.CPU:    resources.Quantity(50),
							sicommon.Memory: resources.Quantity(25)}),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			nr.Allocate(tt.input.appID, tt.input.startTime, tt.input.res)
			if tt.expected.eventNumber != nr.RequestEvents.Len() {
				t.Errorf("events length expect %d, got %d", tt.expected.eventNumber, nr.RequestEvents.Len())
			} else {
				bk := make([]*Event, 0)
				for i := 0; nr.RequestEvents.Len() > 0; i++ {
					tmp := heap.Pop(nr.RequestEvents).(*Event)
					expected := tt.expected.information[i]
					bk = append(bk, tmp)
					if tmp.AppID != expected.appID {
						t.Errorf("%d appID expect %s, got %s", i, expected.appID, tmp.AppID)
					}

					if tmp.Allocate != tt.expected.allocate[i] {
						t.Errorf("%d event should be %v allocate, got %v", i, expected.res, tt.expected.allocate[i])
					}

					if !tmp.Timestamp.Equal(expected.startTime) {
						t.Errorf("%d event timestamp should be %v, got %v", i, expected.startTime, tmp.Timestamp)
					}

					if !resources.Equals(tmp.AllocatedOrRelease, expected.res) {
						t.Errorf("%d resource should be %v, got %v", i, expected.res, tmp.AllocatedOrRelease)
					}
				}
				for _, element := range bk {
					heap.Push(nr.RequestEvents, element)
				}
			}
		})
	}
}
