package users

import (
	"testing"
)

func TestNewScore(t *testing.T) {
	type inputs struct {
		name      string
		weight    int64
		AddWeight int64
	}
	type expection struct {
		name          string
		weight        int64
		UpdatedWeight int64
	}
	tests := []struct {
		caseName string
		input    inputs
		expected expection
	}{
		{"first time", inputs{"yuteng", 0, 100}, expection{"yuteng", 0, 100}},
		{"negative", inputs{"yuteng", -1, 100}, expection{"yuteng", 0, 100}},
		{"normal", inputs{"yuteng", 121, 40}, expection{"yuteng", 121, 161}},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			s := NewScore(test.input.name, test.input.weight)
			if s.GetUser() != test.expected.name || s.GetWeight() != test.expected.weight {
				t.Errorf("Wrong context: expected %s %v, got %s %v", test.expected.name, test.expected.weight, s.GetUser(), s.GetWeight())
			}
			s.AddWeight(test.input.AddWeight)
			if s.GetWeight() != test.expected.UpdatedWeight {
				t.Errorf("Wrong updated weight: expect %d, got %d", test.expected.UpdatedWeight, s.GetWeight())
			}
		})
	}
}
