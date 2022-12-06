package lb

import (
	"github.com/apache/yunikorn-core/pkg/custom/lb/blocks"
	"github.com/apache/yunikorn-core/pkg/custom/lb/topsis"
)

type LBManager struct {
	Nodes map[string]*blocks.PriorityInNode
}

func NewLBManager() *LBManager {
	return &LBManager{
		Nodes: make(map[string]*blocks.PriorityInNode, 0),
	}
}

func (lb *LBManager) AddNode(id string, capacity map[string]int64) {
	lb.Nodes[id] = blocks.NewNode(id, capacity)
}

func (lb *LBManager) Schedule(id string, res map[string]int64, duration uint64) {
	recommanded, startTime := topsis.NewMetaData(topsis.NewAppRequest(id, res, duration), lb.Nodes).Recommanded()
	lb.Nodes[recommanded].Allocate(id, startTime, res, duration)
}

func (lb *LBManager) GetMyNextBacth(nodeID string) []string {
	return lb.Nodes[nodeID].NextBatchToSchedule()
}
