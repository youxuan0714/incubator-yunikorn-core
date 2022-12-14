package lb

import (
	customnode "github.com/apache/yunikorn-core/pkg/custom/lb/node"
	"github.com/apache/yunikorn-core/pkg/custom/lb/topsis"
	"github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"go.uber.org/zap"
	"time"
)

type LBManager struct {
	Nodes map[string]*customnode.NodeResource
}

func NewLBManager() *LBManager {
	return &LBManager{
		Nodes: make(map[string]*customnode.NodeResource, 0),
	}
}

func (lb *LBManager) AddNode(node *objects.Node) {
	nodeID, available := util.ParseNode(node)
	if _, ok := lb.Nodes[nodeID]; !ok {
		log.Logger().Info("LB AddNode", zap.String("node ID", nodeID), zap.Any("capicity", available))
		lb.Nodes[nodeID] = customnode.NewNodeResource(available)
	}
}

// resrouce of application includes CPU, memory and duration.
func (lb *LBManager) Schedule(input *objects.Application, currentTime time.Time) (recommandedNodeID string, startTime time.Time) {
	appID, _, res := util.ParseApp(input)
	recommandedNodeID, startTime = topsis.NewMetaData(appID, currentTime, res, lb.Nodes).Recommanded()
	if node, ok := lb.Nodes[recommandedNodeID]; !ok {
		log.Logger().Warn("LB scheduling decision recommand a non existed node", zap.String("node ID", recommandedNodeID))
	} else {
		node.Allocate(appID, startTime, res)
	}
	return recommandedNodeID, startTime
}
