package blocks

import(
	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

type PriorityInNode struct {
	NodeID   string
	Capacity map[string]int64
	Schedule *Slots
}

func NewNode(id string, capacty map[string]int64) *PriorityInNode {
	return &PriorityInNode{
		NodeID:   id,
		Capacity: capacty,
		Schedule: NewSlots(capacty),
	}
}

func (p *PriorityInNode) NextBatchToSchedule() []string {
	return p.Schedule.BatchApps()
}

func (p *PriorityInNode) GoToNextBatch() {
	if p.Schedule.Len() > 1 {
		p.Schedule.GotoNextSlot()
	} else {
		log.Logger().Warn("GoToNextBatch should not be zero")
	}
}

func (p *PriorityInNode) GetUsageOfTimeT(t uint64) *NodeUsage {
	return p.Schedule.GetUsageOfTimeT(t)
}

func (p *PriorityInNode) Allocate(id string, startTime uint64, res map[string]int64, exeDuration uint64) {
	p.Schedule.Allocate(id, startTime, res, exeDuration)
}

func (p *PriorityInNode) WhenAppCouldBeSchedule(res map[string]int64, exeDuration uint64) uint64 {
	return p.Schedule.TryAllocate(res, exeDuration)
}

func (p *PriorityInNode) Enough(res map[string]int64) bool {
	for key, value := range res {
		if cap, ok := p.Capacity[key]; !ok || cap < value {
			return false
		}
	}
	return true
}
