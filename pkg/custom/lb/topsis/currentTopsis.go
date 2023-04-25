package topsis

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
)

func CurrentTOPSIS(req *resources.Resource, nodes map[string]*node.SimpleNode) string {
	MIGs := make([]float64, 0)
	usages := make([]float64, 0)
	devs := make([]float64, 0)
	nodesID := make([]string, 0)
	for nodeID, n := range nodes {
		nodesID = append(nodesID, nodeID)
		mig, usage := GetObjectives(req, n)
		dev := GetDev(req, nodeID, nodes)
		MIGs = append(MIGs, mig)
		usages = append(usages, usage)
		devs = append(devs, dev)
	}

	// Normalize
	NorMIGs := Normalized(MIGs)
	NorUsages := Normalized(usages)

	objectNames := []string{"usages", "MIG"}
	weightedMIGs := Weight(NorMIGs, objectNames)
	weightedUsages := Weight(NorUsages, objectNames)
	weighted := [][]float64{weightedUsages, weightedMIGs}

	// A+ and A-
	APlusMIG := APlus(weightedMIGs)
	APlusUsages := APlus(weightedUsages)
	AMinusMIG := AMinus(weightedMIGs)
	AMinusUsages := APlus(weightedUsages)

	// SM+ and SM-
	APlusObjective := []float64{APlusUsages, APlusMIG}
	AMinusObjective := []float64{AMinusUsages, AMinusMIG}
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

func GetDev(eq *resources.Resource, assignNode string, nodes map[string]*node.SimpleNode) float64 {
	ns := make([]*resources.Resource, 0)
	for id, n := range nodes {
		res := n.Available
		if id == assignNode {
			res = resources.Sub(res, eq)
		}
		ns = append(ns, resources.CalculateAbsUsedCapacity(n.Capcity, resources.Sub(n.Capcity, res)))
	}
	ave := resources.Average(ns)
	return resources.GetDeviationFromNodes(ns, ave)
}
