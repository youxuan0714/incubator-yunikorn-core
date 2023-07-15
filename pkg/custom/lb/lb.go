package lb

import (
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	customnode "github.com/apache/yunikorn-core/pkg/custom/lb/node"
	"github.com/apache/yunikorn-core/pkg/custom/lb/topsis"
	"github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"go.uber.org/zap"
)

type LBManager struct {
	Nodes  map[string]*customnode.NodeResource
	PNodes map[string]*objects.Node
}

func NewLBManager() *LBManager {
	return &LBManager{
		Nodes:  make(map[string]*customnode.NodeResource, 0),
		PNodes: make(map[string]*objects.Node, 0),
	}
}

func (lb *LBManager) AddNode(node *objects.Node) {
	nodeID, available, cap := util.ParseNode(node)
	if _, ok := lb.Nodes[nodeID]; !ok {
		log.Logger().Info("LB AddNode", zap.String("node ID", nodeID), zap.Any("capicity", available))
		lb.Nodes[nodeID] = customnode.NewNodeResource(available, cap)
		lb.PNodes[nodeID] = node
	}
}

func (lb *LBManager) Allocate(recommandedNodeID, appID string, startTime time.Time, res *resources.Resource) {
	if node, ok := lb.Nodes[recommandedNodeID]; !ok {
		log.Logger().Warn("LB scheduling decision recommand a non existed node", zap.String("node ID", recommandedNodeID))
	} else {
		node.Allocate(appID, startTime, res.Clone())
	}
}

func (lb *LBManager) CurrentSchedule(input *objects.Application) string {
	_, _, res := util.ParseApp(input)
	nodes := lb.GetNodesSimpleNodes(util.ParseAppWithoutDuration(input))
	if len(nodes) == 0 {
		return ""
	}
	return topsis.CurrentTOPSIS(res, nodes)
}

func (lb *LBManager) GetNodesSimpleNodes(request *resources.Resource) map[string]*customnode.SimpleNode {
	results := make(map[string]*customnode.SimpleNode, 0)
	for nodeID, n := range lb.PNodes {
		if n.IsSchedulable() && n.CanAllocate(request) {
			results[nodeID] = customnode.NewSimpleNode(n.GetAvailableResource(), n.GetCapacity(), n.GetUtilizedResource())
		}
	}
	return results
}
