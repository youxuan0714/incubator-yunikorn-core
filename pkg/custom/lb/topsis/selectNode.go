package topsis

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/lb/node"
)

func TOPSIS(req *resources.Resource, nodes map[string]*node.SimpleNode) string {
	MIGs := make([]float64, 0)
	CPUUtilizations := make([]float64, 0)
	MemoryUtilizations := make([]float64, 0)
	//devs := make([]float64, 0)
	nodesID := make([]string, 0)
	for nodeID, targetNode := range nodes {
		nodesID = append(nodesID, nodeID)
		mig := GetMIG(req, targetNode)
		usageOfResource := GetUsages(req, nodeID, nodes)
		//dev := GetDev(req, nodeID, nodes)
		MIGs = append(MIGs, mig)
		CPUUtilizations = append(CPUUtilizations, usageOfResource[0])
		MemoryUtilizations = append(MemoryUtilizations, usageOfResource[1])
		//devs = append(devs, dev)
	}

	// Normalize
	NorCPUs := Normalized(CPUUtilizations)
	NorMems := Normalized(MemoryUtilizations)
	NorMIGs := Normalized(MIGs)
	//NorDevs := Normalized(devs)

	objectNames := []string{"CPUUtilization", "MemoryUtilization", "MIG"}
	weightedCPUs := Weight(NorCPUs, objectNames)
	weightedMems := Weight(NorMems, objectNames)
	weightedMIGs := Weight(NorMIGs, objectNames)
	//weightedDevs := Weight(NorDevs, objectNames)

	// A+ and A-
	APlustCPU := APlusOfUsages(weightedCPUs)
	APlustMem := APlusOfUsages(weightedMems)
	APlusMIG := APlus(weightedMIGs)
	//APlusDevs := APlus(weightedDevs)

	AMinusCPU := AMinusOfUsages(weightedCPUs)
	AMinusMem := AMinusOfUsages(weightedMems)
	AMinusMIG := AMinus(weightedMIGs)
	//AMinusDevs := AMinus(weightedDevs)

	// SM+ and SM-
	weighted := [][]float64{weightedCPUs, weightedMems, weightedMIGs}
	APlusObjective := []float64{APlustCPU, APlustMem, APlusMIG}
	AMinusObjective := []float64{AMinusCPU, AMinusMem, AMinusMIG}
	SMPlusObject := SM(weighted, APlusObjective)
	SMMinusObject := SM(weighted, AMinusObjective)

	nodeIndex, _ := IndexOfMaxRC(SMPlusObject, SMMinusObject)
	return nodesID[nodeIndex]
}

func GetObjectives(req *resources.Resource, n *node.SimpleNode) (float64, float64) {
	// mig float64(resources.GetMIGFromNodeUtilization())
	// usage resources.AverageUsage()
	return GetMIG(req, n), GetNodeUsage(n)
}

func GetNodeUsage(n *node.SimpleNode) float64 {
	return resources.AverageUsage(n.Usage)
}

func GetMIG(req *resources.Resource, n *node.SimpleNode) float64 {
	change := resources.Sub(n.Capcity, resources.Sub(n.Available, req))
	return float64(resources.GetMIGFromNodeUtilization(change))
}

func GetUsages(req *resources.Resource, assignNode string, nodes map[string]*node.SimpleNode) []float64 {
	ns := make([]*resources.Resource, 0)
	for id, n := range nodes {
		res := n.Available
		if id == assignNode {
			res = resources.Sub(res, req)
		}
		ns = append(ns, resources.CalculateAbsUsedCapacity(n.Capcity, resources.Sub(n.Capcity, res)))
	}
	ave := resources.Average(ns)
	return resources.GetCPUandMemoryUtilizations(ave)
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
