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
	if duration > s.GetDuration() {
		t = false
	}
	for key, value := range res {
		if got := s.Avaliable[key]; got < value {
			r = false
			break
		}
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
	if r, t := s.Enough(res, duration); !r {
		return nil, nil
	} else {
		if first {
			s.AddApp(id)
		}
		s2 := s.Clone()
		for key, value := range res {
			s.Avaliable[key] -= value
		}
		if t {
			criticalTime := s.GetStartTime() + duration
			s.EndTime = criticalTime
			s2.StartTime = criticalTime
			return s, s2
		} else {
			return s, nil
		}
	}
}
