package urm

import (
	"testing"
)

func TestNewURM(t *testing.T) {
	tmp := NewURM()
	if len(tmp.existedUser) != 0 && tmp.priority.Len() != 0 {
		t.Error("Default length of existed users and priority is not 0")
	}
}

func TestAddUser(t *testing.T) {
	tests := []struct {
		caseName string
		input    []string
		expected []string
	}{
		{"normal", []string{"user1", "user2", "user3"}, []string{"user1", "user2", "user3"}},
		{"reverse", []string{"user3", "user1", "user2"}, []string{"user1", "user2", "user3"}},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			tmp := NewURM()
			for _, user := range test.input {
				tmp.AddUser(user)
			}
			if len(test.expected) != len(tmp.existedUser) {
				t.Error("existed users is error len")
			} else {
				for _, user := range test.expected {
					if score, ok := tmp.existedUser[user]; !ok {
						t.Errorf("miss score in map %s", user)
					} else {
						if score.GetUser() != user || score.GetWeight() != 0 {
							t.Errorf("expected score %s %v, got %s %v", user, 0, score.GetUser(), score.GetWeight())
						}
					}
				}
			}

			if len(test.expected) != tmp.priority.Len() {
				t.Error("priority is error len")
			} else {
				if minUser := tmp.GetMinResourceUser(); minUser != test.expected[0] {
					t.Errorf("Min user should be %s, got %s", test.expected[0], minUser)
				}
			}
		})
	}
}
