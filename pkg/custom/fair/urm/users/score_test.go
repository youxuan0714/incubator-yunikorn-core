package users

import (
	"testing"
)

func TestNewScore(t *testing.T) {
	type inputs struct {
		name   string
		weight int64
	}
	type expection struct {
		name   string
		weight int64
	}
	tests := []struct {
		caseName string
		input    inputs
		expected expection
	}{
		{"first time", inputs{"yuteng", 0}, expection{"yuteng", 0}},
		{"negative", inputs{"yuteng", -1}, expection{"yuteng", 0}},
		{"normal", inputs{"yuteng", 121}, expection{"yuteng", 121}},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			s := NewScore(test.input.name, test.input.weight)
			if s.GetUser() != test.expected.name || s.GetWeight() != test.expected.weight {
				t.Errorf("Wrong context: expected %s %v, got %s %v", test.expected.name, test.expected.weight, s.GetUser(), s.GetWeight())
			}
		})
	}
}

func TestSumWeight(t *testing.T) {
	type inputs struct {
		name      string
		weight    int64
		resources map[string]int64
	}
	type expection struct {
		name   string
		weight int64
	}
	tests := []struct {
		caseName string
		input    inputs
		expected expection
	}{
		{"first time", inputs{"yuteng", 0, map[string]int64{"cpu": 100, "mem": 200}}, expection{"yuteng", 20000}},
		{"negative", inputs{"yuteng", -1, map[string]int64{"cpu": -100, "mem": 200}}, expection{"yuteng", 0}},
		{"negative2", inputs{"yuteng", -1, map[string]int64{"cpu": 100, "mem": 100}}, expection{"yuteng", 10000}},
		{"normal", inputs{"yuteng", 121, map[string]int64{"cpu": 100, "mem": 100}}, expection{"yuteng", 10121}},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			s := NewScore(test.input.name, test.input.weight)
			s.SumWeight(test.input.resources)
			if s.GetWeight() != test.expected.weight {
				t.Errorf("Expected %d, got %d.", test.expected.weight, s.GetWeight())
			}
		})
	}
}
