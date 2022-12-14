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
