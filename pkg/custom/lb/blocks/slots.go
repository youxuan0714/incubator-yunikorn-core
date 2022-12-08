package blocks

import (
	"container/heap"
)

type Slots []*Slot

func (h Slots) Len() int           { return len(h) }
func (h Slots) Less(i, j int) bool { return h[i].GetStartTime() < h[j].GetStartTime() }
func (h Slots) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Slots) Push(x interface{}) {
	*h = append(*h, x.(*Slot))
}

func (h *Slots) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func NewSlots(capcity map[string]int64) *Slots {
	s := make(Slots, 0)
	heap.Push(&s, NewRootSlot(capcity))
	return &s
}

func (s *Slots) TryAllocate(res map[string]int64, exeDuration uint64) uint64 {
	var startTime uint64
	find := false
	bk := make([]*Slot, 0)
	for s.Len() > 0 && !find {
		// find first slot
		for s.Len() > 0 && !find {
			block := heap.Pop(s).(*Slot)
			if enoughResource, enoughDuration := block.Enough(res, exeDuration); enoughResource {
				startTime = block.GetStartTime()
				heap.Push(s, block)
				if enoughDuration {
					for _, element := range bk {
						heap.Push(s, element)
					}
					find = true
				}
			} else {
				bk = append(bk, block)
			}
		}
		// test sequence
		for sequence, durationReq := true, exeDuration; sequence && !find; {
			block := heap.Pop(s).(*Slot)
			bk = append(bk, block)
			if enoughResource, enoughDuration := block.Enough(res, exeDuration); enoughResource {
				if !enoughDuration {
					durationReq -= block.GetDuration()
				} else {
					for _, element := range bk {
						heap.Push(s, element)
					}
					find = true
				}
			} else {
				// total duration is not enough, try to find next first slot in next round
				sequence = false
			}
		}
	}
	return startTime
}

func (s *Slots) Allocate(id string, startTime uint64, res map[string]int64, exeDuration uint64) {
	bk := make([]*Slot, 0)
	for s.Len() > 0 {
		element := heap.Pop(s).(*Slot)
		if element.StartTime != startTime {
			bk = append(bk, element)
		} else {
			heap.Push(s, element)
		}
	}

	for first, exeDurationReq := true, exeDuration; s.Len() > 0 && exeDurationReq != 0; first = false {
		element := heap.Pop(s).(*Slot)
		front, last := element.Allocate(id, res, exeDuration, first)
		if last == nil {
			bk = append(bk, front)
		} else {
			bk = append(bk, front)
			bk = append(bk, last)
		}
		exeDurationReq -= front.GetDuration()
	}
	for _, element := range bk {
		heap.Push(s, element)
	}
}

func (s *Slots) BatchApps() []string {
	if s.Len() > 1 {
		tmp := heap.Pop(s).(*Slot)
		result := tmp.AppsID
		heap.Push(s, tmp)
		return result
	}
	return []string{}
}

func (s *Slots) GotoNextSlot() {
	if s.Len() > 1 {
		heap.Pop(s)
	}
}

func (s *Slots) GetUsageOfTimeT(t uint64) (result *NodeUsage) {
	bk := make([]*Slot, 0)
	for s.Len() > 0 {
		element := heap.Pop(s).(*Slot)
		if element.GetStartTime() <= t && element.GetEndTime() >= t {
			result = NewNodeUsage(t, element.Avaliable, element.Capacity)
		}
		bk = append(bk, element)
	}
	for _, element := range bk {
		heap.Push(s, element)
	}
	return result
}
