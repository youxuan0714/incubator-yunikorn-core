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
	// Calculate the starttime of each node which could contains application.
	startTimeOfNodes := WhenCanStart(m.Nodes, m.SubmittedTime, m.AppRequest.Clone())

	// stand deviation and mig
	MIGs, standardDeviations, indexOfNodeID := MIGAndStandardDeviation(m.Nodes, startTimeOfNodes, m.AppRequest.Clone())

	//objectNames := []string{"MIG", "Deviation"}
	// normalized
	NorMIGs := Normalized(MIGs)
	NorStandardDeviations := Normalized(standardDeviations)
	weightedMIGs := Weight(NorMIGs)
	weightedStandardDeviations := Weight(NorStandardDeviations)

	// A+ and A-
	APlusMIG := APlus(weightedMIGs)
	APlusStandardDeviation := APlus(weightedStandardDeviations)
	AMinusMIG := AMinus(weightedMIGs)
	AMinusStandardDeviation := AMinus(weightedStandardDeviations)

	// SM+ and SM-
	weighted := [][]float64{weightedMIGs, weightedStandardDeviations}
	APlusObjective := []float64{APlusMIG, APlusStandardDeviation}
	AMinusObjective := []float64{AMinusMIG, AMinusStandardDeviation}
	SMPlusObject := SM(weighted, APlusObjective)
	SMMinusObject := SM(weighted, AMinusObjective)

	// RC
	nodeIndex, _ := IndexOfMaxRC(SMPlusObject, SMMinusObject)
	RecommandednodeID = indexOfNodeID[nodeIndex]
	startTime = startTimeOfNodes[RecommandednodeID]
	return
}

func WhenCanStart(nodes map[string]*node.NodeResource, submittedTime time.Time, app *resources.Resource) map[string]time.Time {
	startTimeOfNodes := make(map[string]time.Time, 0)
	for nodeID, n := range nodes {
		if enough, startTimeOfNode := n.WhenCanStart(submittedTime, app); enough {
			startTimeOfNodes[nodeID] = startTimeOfNode
		}
	}
	return startTimeOfNodes
}

func MIGAndStandardDeviation(nodes map[string]*node.NodeResource, startTimeOfNodes map[string]time.Time, app *resources.Resource) ([]resources.Quantity, []resources.Quantity, []string) {
	MIGs := make([]resources.Quantity, 0)
	standardDeviations := make([]resources.Quantity, 0)
	indexOfNodeID := make([]string, 0)
	for nodeID, startingTime := range startTimeOfNodes {
		indexOfNodeID = append(indexOfNodeID, nodeID)
		utilizationsAtTimeT := make([]*resources.Resource, 0)
		for currentNodeID, n := range nodes {
			if currentNodeID == nodeID {
				// assume that assign application to node and calculate utilization
				AssignedNodeUtilization := n.GetUtilization(startingTime, app)
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
	return MIGs, standardDeviations, indexOfNodeID
}