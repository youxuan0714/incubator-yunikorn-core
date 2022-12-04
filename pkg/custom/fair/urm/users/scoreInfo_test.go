package users

import (
	"testing"
)

func TestNewScoreInfo(t *testing.T) {
	type inputs struct {
		name      string
		resources map[string]int64
	}
	type expection struct {
		name      string
		resources map[string]int64
	}
	tests := []struct {
		caseName string
		input    inputs
		expected expection
	}{
		{"first time", inputs{"yuteng", map[string]int64{"cpu": 100, "mem": 200}}, expection{"yuteng", map[string]int64{"cpu": 100, "mem": 200}}},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			s := NewScoreInfo(test.input.name, test.input.resources)
			if s.GetUser() != test.expected.name {
				t.Errorf("Wrong context: expected %s, got %s", test.expected.name, s.GetUser())
			}
			for key, value := range test.expected.resources {
				if got, ok := s.GetResources()[key]; !ok {
					t.Errorf("Missing resource %s", key)
				} else {
					if value != got {
						t.Errorf("Incorrect resource %s: expected %d, got %d", key, value, got)
					}
				}
			}
		})
	}
}
