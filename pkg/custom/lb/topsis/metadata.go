package topsis

import (
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
	//"github.com/apache/yunikorn-core/pkg/log"
	//"go.uber.org/zap"
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

func (m *MetaData) Recommanded(AppCreateTime time.Time) (RecommandednodeID string, startTime time.Time) {
	// Calculate the starttime of each node which could contains application.
	startTimeOfNodes := WhenCanStart(m.Nodes, m.SubmittedTime, m.AppRequest.Clone())

	// stand deviation and mig
	WaitTimes, MIGs, standardDeviations, distances, indexOfNodeID := MIGAndStandardDeviation(AppCreateTime, m.Nodes, startTimeOfNodes, m.AppRequest.Clone())

	//objectNames := []string{"MIG", "Deviation"}
	// normalized
	NorWaitTimes := Normalized(WaitTimes)
	NorMIGs := Normalized(MIGs)
	NorStandardDeviations := Normalized(standardDeviations)
	NorDistances := Normalized(distances)
	weightedWaitTimes := Weight(NorWaitTimes)
	weightedMIGs := Weight(NorMIGs)
	weightedStandardDeviations := Weight(NorStandardDeviations)
	weightedDistances := Weight(NorDistances)

	// A+ and A-
	APlusWaitTimes := APlus(weightedWaitTimes)
	APlusMIG := APlus(weightedMIGs)
	APlusStandardDeviation := APlus(weightedStandardDeviations)
	APlusDistances := APlus(weightedDistances)
	AMinusWaitTimes := AMinus(weightedWaitTimes)
	AMinusMIG := AMinus(weightedMIGs)
	AMinusStandardDeviation := AMinus(weightedStandardDeviations)
	AMinusDistances := AMinus(weightedDistances)

	// SM+ and SM-
	weighted := [][]float64{weightedWaitTimes, weightedMIGs, weightedStandardDeviations, weightedDistances}
	APlusObjective := []float64{APlusWaitTimes, APlusMIG, APlusStandardDeviation, APlusDistances}
	AMinusObjective := []float64{AMinusWaitTimes, AMinusMIG, AMinusStandardDeviation, AMinusDistances}
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
		if enough, startTimeOfNode := n.WhenCanStart(submittedTime, app.Clone()); enough {
			startTimeOfNodes[nodeID] = startTimeOfNode
			//log.Logger().Info("metadata when", zap.String("nodeID", nodeID), zap.Any("timestamp", startTimeOfNode))
			//log.Logger().Info("expect", zap.String("unassign", n.GetUtilization(startTimeOfNode, nil).String()), zap.String("assign", n.GetUtilization(startTimeOfNode, app.Clone()).String()))
		}
	}
	return startTimeOfNodes
}

func MIGAndStandardDeviation(submitTime time.Time, nodes map[string]*node.NodeResource, startTimeOfNodes map[string]time.Time, app *resources.Resource) ([]float64, []float64, []float64, []float64, []string) {
	WaitTimes := make([]float64, 0)
	MIGs := make([]float64, 0)
	standardDeviations := make([]float64, 0)
	distances := make([]float64, 0)
	indexOfNodeID := make([]string, 0)

	for nodeID, startingTime := range startTimeOfNodes {
		indexOfNodeID = append(indexOfNodeID, nodeID)
		utilizationsAtTimeT := make([]*resources.Resource, 0)
		WaitTimes = append(WaitTimes, startingTime.Sub(submitTime).Seconds())
		for currentNodeID, n := range nodes {
			AssignedNodeUtilization := n.GetUtilization(startingTime, nil)
			if currentNodeID == nodeID {
				// assume that assign application to node and calculate utilization
				AssignedNodeUtilization := n.GetUtilization(startingTime, app)
				MIGs = append(MIGs, float64(resources.MIG(AssignedNodeUtilization)))
			}
			utilizationsAtTimeT = append(utilizationsAtTimeT, AssignedNodeUtilization)
		}
		averageResource := resources.Average(utilizationsAtTimeT)
		standardDeviation := resources.GetDeviationFromNodes(utilizationsAtTimeT, averageResource)
		standardDeviations = append(standardDeviations, standardDeviation)
		distances = append(distances, resources.Distance(averageResource))
	}

	return WaitTimes, MIGs, standardDeviations, distances, indexOfNodeID
}
