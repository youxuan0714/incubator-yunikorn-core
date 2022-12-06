package blocks

type NodeUsage struct {
	TimeStamp uint64
	UsageInfo map[string]*Usage
}

func newNodeUsage(t uint64, usage map[string]*Usage) *NodeUsage {
	return &NodeUsage{
		TimeStamp: t,
		UsageInfo: usage,
	}
}

func (n *NodeUsage) Allocate(res map[string]int64) {
	for key, value := range res {
		target := n.UsageInfo[key]
		target.Allocated += value
		n.UsageInfo[key] = target
	}
}

func (n *NodeUsage) GetUsages() (usages []float64, min float64) {
	usages, min = make([]float64, 0), float64(-1)
	for _, value := range n.UsageInfo {
		result := float64(value.Allocated) / float64(value.Capacity)
		if min == -1 || min > result {
			min = result
		}
		usages = append(usages, float64(value.Allocated)/float64(value.Capacity))
	}
	return
}

type Usage struct {
	Allocated int64
	Capacity  int64
}

func NewUsage(cap, used int64) *Usage {
	return &Usage{
		Allocated: cap - used,
		Capacity:  cap,
	}
}

func NewNodeUsage(timeStamp uint64, avaliable map[string]int64, capacity map[string]int64) *NodeUsage {
	infos := make(map[string]*Usage, 0)
	for key, cap := range capacity {
		tmp := NewUsage(cap, cap-avaliable[key])
		infos[key] = tmp
	}
	return newNodeUsage(timeStamp, infos)
}
