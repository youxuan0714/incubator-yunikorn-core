package topsis

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
)

func CurrentTOPSIS(req *resources.Resource, nodes map[string]*node.SimpleNode) string {
	MIGs := make([]float64, 0)
	usages := make([]float64, 0)
	nodesID := make([]string, 0)
	for nodeID, n := range nodes {
		nodesID = append(nodesID, nodeID)
		mig, usage := GetObjectives(req, n)
		MIGs = append(MIGs, mig)
		usages = append(usages, usage)
	}

	// Normalize
	NorMIGs := Normalized(MIGs)
	// NorUsages := Normalized(usages)

	objectNames := []string{"MIG"}
	weightedMIGs := Weight(NorMIGs, objectNames)
	//weightedUsages := Weight(NorUsages, objectNames)
	weighted := [][]float64{weightedMIGs}

	// A+ and A-
	APlusMIG := APlus(weightedMIGs)
	// APlusUsages := APlus(weightedUsages)
	AMinusMIG := AMinus(weightedMIGs)
	// AMinusUsages := APlus(weightedUsages)

	// SM+ and SM-
	APlusObjective := []float64{APlusMIG}
	AMinusObjective := []float64{AMinusMIG}
	SMPlusObject := SM(weighted, APlusObjective)
	SMMinusObject := SM(weighted, AMinusObjective)

	nodeIndex, _ := IndexOfMaxRC(SMPlusObject, SMMinusObject)
	return nodesID[nodeIndex]
}

func GetObjectives(req *resources.Resource, n *node.SimpleNode) (float64, float64) {
	// mig float64(resources.GetMIGFromNodeUtilization())
	// usage resources.AverageUsage()
	change := resources.Sub(n.Capcity, resources.Sub(n.Available, req))
	mig := float64(resources.GetMIGFromNodeUtilization(change))
	usage := resources.AverageUsage(n.Usage)
	return mig, usage
}
