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
	Nodes map[string]*customnode.NodeResource
}

func NewLBManager() *LBManager {
	return &LBManager{
		Nodes: make(map[string]*customnode.NodeResource, 0),
	}
}

func (lb *LBManager) AddNode(node *objects.Node) {
	nodeID, available, cap := util.ParseNode(node)
	if _, ok := lb.Nodes[nodeID]; !ok {
		log.Logger().Info("LB AddNode", zap.String("node ID", nodeID), zap.Any("capicity", available))
		lb.Nodes[nodeID] = customnode.NewNodeResource(available, cap)
	}
}

// resrouce of application includes CPU, memory and duration.
func (lb *LBManager) Schedule(input *objects.Application, currentTime time.Time) (recommandedNodeID string, startTime time.Time, appID string, res *resources.Resource) {
	appID, _, res = util.ParseApp(input)
	recommandedNodeID, startTime = topsis.NewMetaData(appID, currentTime, res, lb.Nodes).Recommanded(input.SubmissionTime)
	/*if _, ok := lb.Nodes[recommandedNodeID]; !ok {
		log.Logger().Warn("LB scheduling decision recommand a non existed node", zap.Any("time", currentTime), zap.String("node ID", recommandedNodeID))
	}*/
	return recommandedNodeID, startTime, appID, res
}

func (lb *LBManager) Allocate(recommandedNodeID, appID string, startTime time.Time, res *resources.Resource) {
	if node, ok := lb.Nodes[recommandedNodeID]; !ok {
		log.Logger().Warn("LB scheduling decision recommand a non existed node", zap.String("node ID", recommandedNodeID))
	} else {
		node.Allocate(appID, startTime, res.Clone())
	}
}
