package users

type Score struct {
	user   string
	weight int64
}

func NewScore(name string, num int64) *Score {
	weight := num
	if weight < 0 {
		weight = 0
	}

	return &Score{
		user:   name,
		weight: weight,
	}
}

func (s *Score) GetUser() string {
	return s.user
}

func (s *Score) GetWeight() int64 {
	return s.weight
}

func (s *Score) SetWeight(grade int64) {
	s.weight = grade
}

func (s *Score) AddWeight(weight int64) {
	s.weight += weight
}
