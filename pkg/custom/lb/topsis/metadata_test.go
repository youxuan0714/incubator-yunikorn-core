package topsis

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"testing"
	"time"
)

func TestWhenCanStart(t *testing.T) {
	timestamp := time.Now()
	cap := resources.NewResourceFromMap(map[string]resources.Quantity{
		sicommon.CPU:    resources.Quantity(100),
		sicommon.Memory: resources.Quantity(100)})
	app := resources.NewResourceFromMap(map[string]resources.Quantity{
		sicommon.CPU:      resources.Quantity(100),
		sicommon.Memory:   resources.Quantity(100),
		sicommon.Duration: resources.Quantity(100)})

	nodes := map[string]*node.NodeResource{
		"node-1": node.NewNodeResource(cap.Clone(), cap.Clone()),
		"node-2": node.NewNodeResource(cap.Clone(), cap.Clone()),
		"node-3": node.NewNodeResource(cap.Clone(), cap.Clone()),
		"node-4": node.NewNodeResource(cap.Clone(), cap.Clone()),
	}
	nodes["node-2"].Allocate("test", timestamp, app.Clone())
	nodes["node-4"].Allocate("test", timestamp, app.Clone())

	expect := map[string]time.Time{
		"node-1": timestamp,
		"node-2": timestamp.Add(time.Second * 100),
		"node-3": timestamp,
		"node-4": timestamp.Add(time.Second * 100),
	}

	startTimes := WhenCanStart(nodes, timestamp, app.Clone())
	for nodeID, startTime := range startTimes {
		if !expect[nodeID].Equal(startTime) {
			t.Errorf("%v: %s expect %v, got %v", timestamp, nodeID, expect[nodeID], startTime)
		}
	}
	/*
		expect = map[string]time.Time{
			"node-1": timestamp.Add(time.Second * 100),
			"node-2": timestamp.Add(time.Second * 100),
			"node-3": timestamp.Add(time.Second * 100),
			"node-4": timestamp.Add(time.Second * 100),
		}
		startTimes = WhenCanStart(nodes, timestamp.Add(time.Second*50), app.Clone())
		for nodeID, startTime := range startTimes {
			if !expect[nodeID].Equal(startTime) {
				t.Errorf("%v: %s expect %v, got %v", timestamp.Add(time.Second*100), nodeID, expect[nodeID], startTime)
			}
		}
	*/
}
