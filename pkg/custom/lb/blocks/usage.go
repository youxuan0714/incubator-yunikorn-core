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
