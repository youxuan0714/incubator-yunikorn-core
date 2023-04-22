package topsis

import (
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	//"github.com/apache/yunikorn-core/pkg/log"
	//"go.uber.org/zap"
)

type MetaData struct {
	AppID         string
	SubmittedTime time.Time
	AppRequest    *resources.Resource           // include cpu, memory and duration
	Nodes         map[string]*node.NodeResource // includes cpu, memory
	EndingTime    time.Time
	Makespan      float64
}

func NewMetaData(appID string, submittedTime time.Time, app *resources.Resource, nodes map[string]*node.NodeResource) *MetaData {
	return &MetaData{
		AppID:         appID,
		SubmittedTime: submittedTime,
		AppRequest:    app.Clone(),
		Nodes:         nodes,
		EndingTime:    time.Now(),
		Makespan:      0.0,
	}
}

func (m *MetaData) Recommanded(AppCreateTime time.Time) (RecommandednodeID string, startTime time.Time) {
	// Calculate the starttime of each node which could contains application.
	startTimeOfNodes := WhenCanStart(m.Nodes, m.SubmittedTime, m.AppRequest.Clone())
	if len(startTimeOfNodes) == 0 {
		return "", time.Now()
	}

	// stand deviation and mig
	_, MIGs, standardDeviations, _, distances, makespans, indexOfNodeID := MIGAndStandardDeviation(AppCreateTime, m.Nodes, startTimeOfNodes, m.AppRequest.Clone(), m.EndingTime, m.Makespan)

	// normalized
	//NorWaitTimes := Normalized(WaitTimes)
	NorMIGs := Normalized(MIGs)
	NorStandardDeviations := Normalized(standardDeviations)
	//NorUsages := Normalized(usages)
	NorDistances := Normalized(distances)
	NorMakespans := Normalized(makespans)
	objectNames := []string{"mig", "dev", "makespan", "distance"}
	//weightedWaitTimes := Weight(NorWaitTimes, objectNames)
	weightedMIGs := Weight(NorMIGs, objectNames)
	weightedStandardDeviations := Weight(NorStandardDeviations, objectNames)
	//weightedUsages := Weight(NorUsages, objectNames)
	weightedDistances := Weight(NorDistances, objectNames)
	weightedMakespans := Weight(NorMakespans, objectNames)

	// A+ and A-
	//APlusWaitTimes := APlus(weightedWaitTimes)
	APlusMIG := APlus(weightedMIGs)
	APlusStandardDeviation := APlus(weightedStandardDeviations)
	APlusMakespans := APlus(weightedMakespans)
	// APlusUsages := APlus(weightedUsages)
	APlusDistances := APlus(weightedDistances)
	// AMinusWaitTimes := AMinus(weightedWaitTimes)
	AMinusMIG := AMinus(weightedMIGs)
	AMinusStandardDeviation := AMinus(weightedStandardDeviations)
	AMinusDistances := AMinus(weightedDistances)
	AMinusMakespans := AMinus(weightedMakespans)
	// AMinusUsages := APlus(weightedUsages)

	// SM+ and SM-
	weighted := [][]float64{weightedMIGs, weightedStandardDeviations, weightedMakespans, weightedDistances}
	APlusObjective := []float64{APlusMIG, APlusStandardDeviation, APlusMakespans, APlusDistances}
	AMinusObjective := []float64{AMinusMIG, AMinusStandardDeviation, AMinusMakespans, AMinusDistances}
	SMPlusObject := SM(weighted, APlusObjective)
	SMMinusObject := SM(weighted, AMinusObjective)

	// RC
	nodeIndex, _ := IndexOfMaxRC(SMPlusObject, SMMinusObject)
	RecommandednodeID = indexOfNodeID[nodeIndex]
	startTime = startTimeOfNodes[RecommandednodeID]

	duration := time.Duration(int64(m.AppRequest.Resources[sicommon.Duration]))
	m.EndingTime = startTime.Add(duration)
	m.Makespan += float64(int64(m.EndingTime.Sub(startTime)))
	return
}

func WhenCanStart(nodes map[string]*node.NodeResource, submittedTime time.Time, app *resources.Resource) map[string]time.Time {
	startTimeOfNodes := make(map[string]time.Time, 0)
	for nodeID, n := range nodes {
		if enough, startTimeOfNode := n.WhenCanStart(submittedTime, app.Clone()); enough && startTimeOfNode.Equal(submittedTime) {
			startTimeOfNodes[nodeID] = startTimeOfNode
			//log.Logger().Info("metadata when", zap.String("nodeID", nodeID), zap.Any("timestamp", startTimeOfNode))
			//log.Logger().Info("expect", zap.String("unassign", n.GetUtilization(startTimeOfNode, nil).String()), zap.String("assign", n.GetUtilization(startTimeOfNode, app.Clone()).String()))
		}
	}
	return startTimeOfNodes
}

func MIGAndStandardDeviation(submitTime time.Time, nodes map[string]*node.NodeResource, startTimeOfNodes map[string]time.Time, app *resources.Resource, end time.Time, makespan float64) ([]float64, []float64, []float64, []float64, []float64, []float64, []string) {
	WaitTimes := make([]float64, 0)
	MIGs := make([]float64, 0)
	standardDeviations := make([]float64, 0)
	distances := make([]float64, 0)
	usages := make([]float64, 0)
	indexOfNodeID := make([]string, 0)
	makespans := make([]float64, 0)
	duration := time.Duration(int64(app.Resources[sicommon.Duration]))

	for nodeID, startingTime := range startTimeOfNodes {
		makespans = append(makespans, float64(int64(startingTime.Add(duration).Sub(end)))+makespan)
		indexOfNodeID = append(indexOfNodeID, nodeID)
		utilizationsAtTimeT := make([]*resources.Resource, 0)
		WaitTimes = append(WaitTimes, startingTime.Sub(submitTime).Seconds())
		for currentNodeID, n := range nodes {
			AssignedNodeUtilization := n.GetUtilization(startingTime, nil)
			if currentNodeID == nodeID {
				usages = append(usages, resources.AverageUsage(AssignedNodeUtilization))
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

	return WaitTimes, MIGs, standardDeviations, distances, makespans, usages, indexOfNodeID
}
