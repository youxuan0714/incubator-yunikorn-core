package monitor

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"go.uber.org/zap"
)

type NodeUtilizationMonitor struct {
	nodes map[string]*objects.Node
}

func NewUtilizationMonitor() *NodeUtilizationMonitor {
	return &NodeUtilizationMonitor{
		nodes: make(map[string]*objects.Node, 0),
	}
}

func (m *NodeUtilizationMonitor) TraceNodes() {
	for nodeID, node := range m.nodes {
		mig := resources.MIG(node.GetUtilizedResource())
		log.Logger().Info("Trace MIG", zap.String("nodeID", nodeID), zap.Any("mig value", mig))
	}
}

func (m *NodeUtilizationMonitor) AddNode(n *objects.Node) {
	nodeID := n.NodeID
	if _, ok := m.nodes[nodeID]; !ok {
		m.nodes[nodeID] = n
	}
}
