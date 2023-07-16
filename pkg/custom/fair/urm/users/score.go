package users

type Score struct {
	user   string
	weight float64
}

func NewScore(name string, num float64) *Score {
	weight := num
	if weight < 0.0 {
		weight = 0.0
	}

	return &Score{
		user:   name,
		weight: weight,
	}
}

func (s *Score) GetUser() string {
	return s.user
}

func (s *Score) GetWeight() float64 {
	return s.weight
}
