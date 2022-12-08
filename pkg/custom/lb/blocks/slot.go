package blocks

const (
	MaxUint = ^uint64(0)
	MinUint = uint64(0)
)

type Slot struct {
	StartTime uint64
	EndTime   uint64
	Capacity  map[string]int64
	Avaliable map[string]int64
	AppsID    []string
}

func (s *Slot) GetStartTime() uint64 {
	return s.StartTime
}

func (s *Slot) GetEndTime() uint64 {
	return s.EndTime
}

func (s *Slot) GetDuration() uint64 {
	return s.GetEndTime() - s.GetStartTime()
}

func (s *Slot) Enough(res map[string]int64, duration uint64) (bool, bool) {
	r, t := true, true
	for key, value := range res {
		if got := s.Avaliable[key]; got < value {
			r = false
			return r, t
		}
	}

	if duration > s.GetDuration() {
		t = false
	}
	return r, t
}

func NewRootSlot(capacity map[string]int64) *Slot {
	return &Slot{
		StartTime: MinUint,
		EndTime:   MaxUint,
		Avaliable: capacity,
		Capacity:  capacity,
		AppsID:    make([]string, 0),
	}
}

func (s *Slot) Clone() *Slot {
	return &Slot{
		StartTime: s.StartTime,
		EndTime:   s.EndTime,
		Avaliable: s.Avaliable,
		Capacity:  s.Capacity,
		AppsID:    make([]string, 0),
	}
}

func (s *Slot) AddApp(id string) {
	s.AppsID = append(s.AppsID, id)
}

func (s *Slot) Allocate(id string, res map[string]int64, duration uint64, first bool) (*Slot, *Slot) {
	var r, t bool
	if r, t = s.Enough(res, duration); !r {
		return nil, nil
	}

	if first {
		s.AddApp(id)
	}

	var s2 *Slot
	if criticalTime := s.GetStartTime() + duration; t {
		s2 = s.Clone()
		s.EndTime = criticalTime
		s2.StartTime = criticalTime
	} else {
		s2 = nil
	}

	for key, value := range res {
		s.Avaliable[key] -= value
	}
	return s, s2
}
