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
	nr := NewNodeResource(cap.Clone(), cap.Clone())
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

func TestAllocateAndWhenCanStart(t *testing.T) {
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

	type outputFormat2 struct {
		startTime time.Time
		enough    bool
	}
	tests2 := []struct {
		caseName string
		input    inputFormat
		expect   outputFormat2
	}{
		{
			caseName: "Excess the capicity",
			input: inputFormat{
				appID:     "test-03",
				startTime: timestamp,
				res: resources.NewResourceFromMap(map[string]resources.Quantity{
					sicommon.CPU:      resources.Quantity(150),
					sicommon.Memory:   resources.Quantity(25),
					sicommon.Duration: resources.Quantity(5)}),
			},
			expect: outputFormat2{
				startTime: timestamp,
				enough:    false,
			},
		},
		{
			caseName: "Allication at t = 5",
			input: inputFormat{
				appID:     "test-03",
				startTime: timestamp.Add(time.Second * 5),
				res: resources.NewResourceFromMap(map[string]resources.Quantity{
					sicommon.CPU:      resources.Quantity(50),
					sicommon.Memory:   resources.Quantity(25),
					sicommon.Duration: resources.Quantity(5)}),
			},
			expect: outputFormat2{
				startTime: timestamp.Add(time.Second * 25),
				enough:    true,
			},
		},
		{
			caseName: "Allication at t = 25",
			input: inputFormat{
				appID:     "test-03",
				startTime: timestamp.Add(time.Second * 25),
				res: resources.NewResourceFromMap(map[string]resources.Quantity{
					sicommon.CPU:      resources.Quantity(50),
					sicommon.Memory:   resources.Quantity(25),
					sicommon.Duration: resources.Quantity(5)}),
			},
			expect: outputFormat2{
				startTime: timestamp.Add(time.Second * 25),
				enough:    true,
			},
		},
		{
			caseName: "Allication at t = 100",
			input: inputFormat{
				appID:     "test-03",
				startTime: timestamp.Add(time.Second * 100),
				res: resources.NewResourceFromMap(map[string]resources.Quantity{
					sicommon.CPU:      resources.Quantity(50),
					sicommon.Memory:   resources.Quantity(25),
					sicommon.Duration: resources.Quantity(5)}),
			},
			expect: outputFormat2{
				startTime: timestamp.Add(time.Second * 100),
				enough:    true,
			},
		},
	}
	for _, tt := range tests2 {
		t.Run(tt.caseName, func(t *testing.T) {
			enough, startTime := nr.WhenCanStart(tt.input.startTime, tt.input.res)
			if enough != tt.expect.enough {
				t.Errorf("Node should not acess the req %v which is bigger than capicity %v", tt.input.res.Clone(), nr.Capcity.Clone())
			} else {
				if !startTime.Equal(tt.expect.startTime) {
					t.Errorf("Expect application would start at %v, not %v", tt.expect.startTime, startTime)
				}
			}
		})
	}
}

func TestClearEventsBaseOnSubmittedTime(t *testing.T) {
	timestamp := time.Now()
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
				sicommon.CPU:      resources.Quantity(50),
				sicommon.Memory:   resources.Quantity(25),
				sicommon.Duration: resources.Quantity(50)}),
		},
	}
	tests := []struct {
		caseName      string
		submittedTime time.Time
		elementNum    int
		NewTimestamp  time.Time
		expectedRes   *resources.Resource
	}{
		{
			caseName:      "t = 0, should handle two allication ask",
			submittedTime: timestamp,
			elementNum:    2,
			NewTimestamp:  timestamp,
			expectedRes: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:    resources.Quantity(0),
				sicommon.Memory: resources.Quantity(50)}),
		},
		{
			caseName:      "t = 25, should handle two allication ask and a release",
			submittedTime: timestamp.Add(time.Second * 25),
			elementNum:    1,
			NewTimestamp:  timestamp.Add(time.Second * 25),
			expectedRes: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:    resources.Quantity(50),
				sicommon.Memory: resources.Quantity(75)}),
		},
		{
			caseName:      "t = 100, should handle two allication ask and two release",
			submittedTime: timestamp.Add(time.Second * 100),
			elementNum:    0,
			NewTimestamp:  timestamp.Add(time.Second * 50),
			expectedRes: resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:    resources.Quantity(100),
				sicommon.Memory: resources.Quantity(100)}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			cap := resources.NewResourceFromMap(map[string]resources.Quantity{
				sicommon.CPU:    resources.Quantity(100),
				sicommon.Memory: resources.Quantity(100),
			})
			nr := NewNodeResource(cap.Clone(), cap.Clone())
			nr.CurrentTime = timestamp
			for _, allocation := range allocations {
				nr.Allocate(allocation.appID, allocation.startTime, allocation.res)
			}
			/*
				if i == 2 || i == 1 {
					bk := make([]*Event, 0)
					for nr.RequestEvents.Len() > 0 {
						tmp := heap.Pop(nr.RequestEvents).(*Event)
						t.Errorf("%v, %v, %v", tmp.Timestamp, tt.submittedTime, tt.NewTimestamp)
					}
					for _, element := range bk {
						heap.Push(nr.RequestEvents, element)
					}
				}
			*/
			nr.ClearEventsBaseOnSubmittedTime(tt.submittedTime)
			if nr.RequestEvents.Len() != tt.elementNum {
				t.Errorf("element number in %v expect %v, got %v", nr.RequestEvents, tt.elementNum, nr.RequestEvents.Len())
			}
			if !nr.CurrentTime.Equal(tt.NewTimestamp) {
				t.Errorf("timestamp expect %v, got %v", tt.NewTimestamp, nr.CurrentTime)
			}
			if !resources.Equals(nr.Available, tt.expectedRes) {
				t.Errorf("Resource expected %v, got %v", tt.expectedRes, nr.Available)
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
