package topsis

import (
	"fmt"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"testing"
	"time"
)

func TestWhenCanStart(t *testing.T) {
	timestamp := time.Now()
	app := resources.NewResourceFromMap(map[string]resources.Quantity{
		sicommon.CPU:      resources.Quantity(100),
		sicommon.Memory:   resources.Quantity(100),
		sicommon.Duration: resources.Quantity(100)})
	nodes := make(map[string]*node.NodeResource, 0)
	for i := 0; i < 4; i++ {
		tmp := node.NewNodeResource(resources.NewResourceFromMap(map[string]resources.Quantity{
			sicommon.CPU:    resources.Quantity(100),
			sicommon.Memory: resources.Quantity(100)}))
		tmp.CurrentTime = timestamp
		if i%2 == 0 {
			tmp.Allocate("test", timestamp, app)
		}
		nodes[fmt.Sprintf("node-%d", i)] = tmp
	}

	index := 0
	startTimes := WhenCanStart(nodes, timestamp, app)
	for nodeID, startTime := range startTimes {
		if index%2 == 0 {
			expect := timestamp.Add(time.Duration(time.Second * 100))
			if !startTime.Equal(expect) {
				t.Errorf("%s expect %v, got %v", nodeID, expect, startTime)
			}
		} else {
			expect := timestamp
			if !expect.Equal(startTime) {
				t.Errorf("%s same expect %v, got %v", nodeID, expect, startTime)
			}
		}
		index++
	}
	startTimes = WhenCanStart(nodes, timestamp.Add(time.Second*100), app)
	index = 0
	for nodeID, startTime := range startTimes {
		if expect := timestamp.Add(time.Duration(time.Second * 100)); !expect.Equal(startTime) {
			t.Errorf("%s same expect %v, got %v", nodeID, expect, startTime)
		}
		index++
	}
}
