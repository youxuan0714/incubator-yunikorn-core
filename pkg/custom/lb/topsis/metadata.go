package topsis

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
	"time"
)

type MetaData struct {
	AppID         string
	SubmittedTime time.Time
	AppRequest    *resources.Resource           // include cpu, memory and duration
	Nodes         map[string]*node.NodeResource //includes cpu, memory
}

func NewMetaData(appID string, submittedTime time.Time, app *resources.Resource, nodes map[string]*node.NodeResource) *MetaData {
	return &MetaData{
		AppID:         appID,
		SubmittedTime: submittedTime,
		AppRequest:    app.Clone(),
		Nodes:         nodes,
	}
}

func (m *MetaData) Recommanded() (RecommandednodeID string, startTime time.Time) {
	MIGs := make([]resources.Quantity, 0)
	standardDeviations := make([]resources.Quantity, 0)
	startTimeOfNodes := make(map[string]time.Time, 0)
	// Calculate the starttime of each node which could contains application.
	for nodeID, n := range m.Nodes {
		if enough, startTimeOfNode := n.WhenCanStart(m.SubmittedTime, m.AppRequest); enough {
			startTimeOfNodes[nodeID] = startTimeOfNode
		}
	}

	// stand deviation and mig
	indexOfNodeID := make([]string, 0)
	for nodeID, startingTime := range startTimeOfNodes {
		indexOfNodeID = append(indexOfNodeID, nodeID)
		utilizationsAtTimeT := make([]*resources.Resource, 0)
		for currentNodeID, n := range m.Nodes {
			if currentNodeID == nodeID {
				// assume that assign application to node and calculate utilization
				AssignedNodeUtilization := n.GetUtilization(startingTime, m.AppRequest.Clone())
				utilizationsAtTimeT = append(utilizationsAtTimeT, AssignedNodeUtilization)
				MIGs = append(MIGs, resources.MIG(AssignedNodeUtilization))
			} else {
				utilizationsAtTimeT = append(utilizationsAtTimeT, n.GetUtilization(startingTime, nil))
			}
		}
		averageResource := resources.Average(utilizationsAtTimeT)
		gapSum := resources.NewResource()
		// sum += (utilization - average utilization)^2
		for _, n := range utilizationsAtTimeT {
			gap := resources.Sub(n, averageResource)
			powerGap := resources.Power(gap, float64(2))
			gapSum = resources.Add(gapSum, powerGap)
		}
		// Max deviation = Max(SQRT(sum including cpu and memory))
		gapSum = resources.Power(gapSum, float64(0.5))
		standardDeviation := resources.Max(gapSum)
		standardDeviations = append(standardDeviations, standardDeviation)
	}

	objectNames := []string{"MIG", "Deviation"}
	// normalized
	NorMIGs := Normalized(MIGs)
	NorStandardDeviations := Normalized(standardDeviations)
	weighted := Weight(objectNames, NorMIGs, NorStandardDeviations, float64(2))

	// A+ and A-
	APlusObjects := APlus(objectNames, weighted)
	AMinusObjects := AMinus(objectNames, weighted)

	// SM+ and SM-
	SMPlusObject := SM(objectNames, weighted, APlusObjects)
	SMMinusObject := SM(objectNames, weighted, AMinusObjects)

	// RC
	nodeIndex := IndexOfMaxRC(SMPlusObject, SMMinusObject)
	RecommandednodeID = indexOfNodeID[nodeIndex]
	startTime = startTimeOfNodes[RecommandednodeID]
	return
}
