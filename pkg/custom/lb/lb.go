package lb

import (
	"github.com/apache/yunikorn-core/pkg/custom/lb/blocks"
	"github.com/apache/yunikorn-core/pkg/custom/lb/topsis"
	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
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
	if _, ok := lb.Nodes[id]; !ok {
		log.Logger().Info("LB AddNode", zap.String("node ID", id), zap.Any("capicity", capacity))
		lb.Nodes[id] = blocks.NewNode(id, capacity)
	}
}

func (lb *LBManager) Schedule(appID string, res map[string]int64, duration uint64) {
	recommanded, startTime := topsis.NewMetaData(topsis.NewAppRequest(appID, res, duration), lb.Nodes).Recommanded()
	if node, ok := lb.Nodes[recommanded]; !ok {
		log.Logger().Warn("LB scheduling decision recommand a non existed node", zap.String("node ID", recommanded))
	} else {
		node.Allocate(appID, startTime, res, duration)
	}
}

func (lb *LBManager) GetNodeNextBacth(nodeID string) []string {
	if node, ok := lb.Nodes[nodeID]; !ok {
		log.Logger().Warn("Next batch of a non existed node", zap.String("node ID", nodeID))
		return []string{}
	} else {
		return node.NextBatchToSchedule()
	}
}

func (lb *LBManager) GoToNextBatch(nodeID string) {
	if node, ok := lb.Nodes[nodeID]; ok {
		node.GoToNextBatch()
	} else {
		log.Logger().Warn("Non existed node should not execute goToNextBatch", zap.String("node ID", nodeID))
	}
}
