package topsis

import (
	"math"

	"github.com/apache/yunikorn-core/pkg/custom/lb/blocks"
)

type MetaData struct {
	App   *AppRequest
	Nodes map[string]*blocks.PriorityInNode
}

func NewMetaData(app *AppRequest, nodes map[string]*blocks.PriorityInNode) *MetaData {
	return &MetaData{
		App:   app,
		Nodes: nodes,
	}
}

type AppRequest struct {
	Id       string
	Res      map[string]int64
	Duration uint64
}

func NewAppRequest(id string, res map[string]int64, duration uint64) *AppRequest {
	return &AppRequest{
		Id:       id,
		Res:      res,
		Duration: duration,
	}
}

func (m *MetaData) Recommanded() (string, uint64) {
	startTimes, nodes := m.StartTimes()
	candicates := make([]string, 0)
	candicatesTime := make([]uint64, 0)
	migs := make([]float64, 0)
	biases := make([]float64, 0)
	for assignedNodeNum, timeStamp := range startTimes {
		if !m.Nodes[nodes[assignedNodeNum]].Enough(m.App.Res) {
			continue
		} else {
			candicates = append(candicates, nodes[assignedNodeNum])
			candicatesTime = append(candicatesTime, timeStamp)
		}
		statuses := make([]*blocks.NodeUsage, 0)
		for nodeID, node := range m.Nodes {
			assignedNode := node.GetUsageOfTimeT(timeStamp)
			if nodes[assignedNodeNum] == nodeID {
				assignedNode.Allocate(m.App.Res)
				migs = append(migs, MIG(assignedNode))
			}
			statuses = append(statuses, assignedNode)
		}
		biases = append(biases, MaxBias(statuses))
	}
	migs, biases = NormalizedAndWeight(migs, biases)
	migMinus, migPlus := MaxAndMin(migs)
	biasMinus, biasPlus := MaxAndMin(biases)
	target := SMAndRecommded(migs, biases, []float64{migPlus, biasPlus}, []float64{migMinus, biasMinus})
	return candicates[target], candicatesTime[target]
}

func SMAndRecommded(migs, biases, plus, minus []float64) (maxIndex int) {
	var max float64
	for i := 0; i < len(migs); i++ {
		smPlus := math.Sqrt(math.Pow(migs[i]-plus[0], 2) + math.Pow(biases[i]-plus[1], 2))
		smMinus := math.Sqrt(math.Pow(migs[i]-minus[0], 2) + math.Pow(biases[i]-minus[1], 2))
		rc := smMinus / (smMinus + smPlus)
		if i == 0 || rc > max {
			max = rc
			maxIndex = i
		}
	}
	return
}

func NormalizedAndWeight(migs []float64, biases []float64) ([]float64, []float64) {
	baseMIG, baseBias := float64(0), float64(0)
	weight := float64(2)
	for i := 0; i < len(migs); i++ {
		baseMIG += math.Pow(migs[i], 2)
		baseBias += math.Pow(biases[i], 2)
	}
	baseBias = math.Sqrt(baseBias) * weight
	baseMIG = math.Sqrt(baseMIG) * weight
	for i := 0; i < len(migs); i++ {
		migs[i] /= baseMIG
		biases[i] /= baseBias
	}
	return migs, biases
}

func (m *MetaData) StartTimes() ([]uint64, []string) {
	nodes := make([]string, 0)
	results := make([]uint64, 0)
	for id, node := range m.Nodes {
		results = append(results, node.WhenAppCouldBeSchedule(m.App.Res, m.App.Duration))
		nodes = append(nodes, id)
	}
	return results, nodes
}

func MIG(node *blocks.NodeUsage) float64 {
	usages, min := node.GetUsages()
	sum := float64(0)
	for _, usage := range usages {
		sum += (usage - min)
	}
	return sum
}

func MaxBias(nodes []*blocks.NodeUsage) float64 {
	average := make([]float64, 0)
	for index, node := range nodes {
		usages, _ := node.GetUsages()
		if index == 0 {
			average = append(average, usages...)
			continue
		}
		for resourceType, usage := range usages {
			average[resourceType] += usage
		}
	}

	for i := 0; i < len(average); i++ {
		average[i] /= float64(len(nodes))
	}

	sum := make([]float64, 2)
	for _, node := range nodes {
		usages, _ := node.GetUsages()
		for resourceType, usage := range usages {
			sum[resourceType] += math.Pow(usage-average[resourceType], 2)
		}
	}

	for i := 0; i < len(sum); i++ {
		sum[i] = math.Sqrt(sum[i])
	}
	max, _ := MaxAndMin(sum)
	return max
}

func CalculateSM(normalized, aPlus, aMinus []float64) (smPlus float64, smMinus float64) {
	smPlus, smMinus = float64(0), float64(0)
	for index, base := range aPlus {
		smPlus += math.Pow(normalized[index]-base, 2)
	}
	for index, base := range aMinus {
		smPlus += math.Pow(normalized[index]-base, 2)
	}
	smPlus = math.Sqrt(smPlus)
	smMinus = math.Sqrt(smMinus)
	return
}

func MaxAndMin(items []float64) (max float64, min float64) {
	min = items[0]
	max = items[0]
	for _, item := range items {
		if min > item {
			min = item
		}
		if max < item {
			max = item
		}
	}
	return
}
