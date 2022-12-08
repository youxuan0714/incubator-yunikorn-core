package blocks

import (
	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

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
		if target, ok := n.UsageInfo[key]; !ok {
			log.Logger().Error("key in nodeUsage Allocate is not existed", zap.String("resrouce", key))
		} else {
			target.Allocated += value
			n.UsageInfo[key] = target
		}
	}
}

func (n *NodeUsage) GetUsages() (usages []float64, min float64) {
	usages, min = make([]float64, 0), float64(-1)
	for _, value := range n.UsageInfo {
		got := float64(value.Allocated) / float64(value.Capacity)
		if min == -1 || min > got {
			min = got
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

func NewNodeUsage(timeStamp uint64, available map[string]int64, capacity map[string]int64) *NodeUsage {
	infos := make(map[string]*Usage, 0)
	for key, cap := range capacity {
		if avail, ok := available[key]; !ok {
			log.Logger().Error("avaiable is not existed", zap.String("resrouce", key))
		} else {
			infos[key] = NewUsage(cap, cap-avail)
		}
	}
	return newNodeUsage(timeStamp, infos)
}
