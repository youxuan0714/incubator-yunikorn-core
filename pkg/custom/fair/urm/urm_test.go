package urm

import (
	"container/heap"
	"testing"

	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/users"
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

func TestUpdateMinUser(t *testing.T) {
	tests := []struct {
		caseName       string
		input          []string
		inputRes       []map[string]int64
		expected       []string
		expectedWeight []int64
	}{
		{"normal",
			[]string{"user1", "user2", "user3"},
			[]map[string]int64{
				map[string]int64{"vcore": 100, "memory": 200, "duration": 5},
				map[string]int64{"vcore": 100, "memory": 100, "duration": 2},
				map[string]int64{"vcore": 100, "memory": 50, "duration": 3},
			},
			[]string{"user3", "user2", "user1"},
			[]int64{15000, 20000, 100000},
		},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			tmp := NewURM()
			for _, user := range test.input {
				tmp.AddUser(user)
			}
			for _, res := range test.inputRes {
				if err := tmp.UpdateUser(users.NewScoreInfo(tmp.GetMinResourceUser(), res)); err != nil {
					t.Error(err)
				}
			}
			if len(test.expected) != len(tmp.existedUser) {
				t.Error("existed users is error len")
			} else {
				for index, user := range test.expected {
					if score, ok := tmp.existedUser[user]; !ok {
						t.Errorf("miss score in map %s\n", user)
					} else {
						if score.GetUser() != user || score.GetWeight() != test.expectedWeight[index] {
							t.Errorf("expected score %s %v, got %s %v\n", user, test.expectedWeight[index], score.GetUser(), score.GetWeight())
						}
					}
				}
			}
			for i := 0; tmp.priority.Len() > 0; i++ {
				if tmp.GetMinResourceUser() != test.expected[i] {
					t.Errorf("expected min user: %s, got %s", test.expected[i], tmp.GetMinResourceUser())
				}
				s := heap.Pop(tmp.priority).(*users.Score)
				if s.GetUser() != test.expected[i] || s.GetWeight() != test.expectedWeight[i] {
					t.Errorf("Min order: expected %s %v, got %s %v", test.expected[i], test.expectedWeight[i], s.GetUser(), s.GetWeight())
				}
			}
		})
	}
}
