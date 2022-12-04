package users

type ScoreInfo struct {
	user      string
	resources map[string]int64
}

func NewScoreInfo(name string, resources map[string]int64) *ScoreInfo {
	return &ScoreInfo{
		user:      name,
		resources: resources,
	}
}

func (s *ScoreInfo) GetUser() string {
	return s.user
}

func (s *ScoreInfo) GetResources() map[string]int64 {
	return s.resources
}
