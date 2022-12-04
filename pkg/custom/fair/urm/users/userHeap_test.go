package users

import (
	"container/heap"
	"testing"
)

func TestNewUserHeap(t *testing.T) {
	h := NewUserHeap()
	if h.Len() != 0 {
		t.Error("default len of user heap should be 0")
	}
}

func TestPush(t *testing.T) {
	h := NewUserHeap()
	names := []string{"user1", "user2"}
	bk := make([]*Score, 0)
	info := NewScoreInfo("user1", map[string]int64{"cpu": 100, "mem": 100})
	heap.Push(h, NewScore(names[0], 0))
	heap.Push(h, NewScore(names[1], 0))
	heap.Init(h)
	for _, name := range names {
		tmp := heap.Pop(h).(*Score)
		if tmp.GetUser() != name && tmp.GetWeight() != 0 {
			t.Errorf("Wrong user %s %d", tmp.GetUser(), tmp.GetWeight())
		}
		if info.GetUser() == tmp.GetUser() {
			tmp.SumWeight(info.GetResources())
		}
		bk = append(bk, tmp)
	}

	for _, value := range bk {
		heap.Push(h, value)
	}

	names = []string{"user2", "user1"}
	weights := []int64{0, 10000}
	for index, name := range names {
		tmp := heap.Pop(h).(*Score)
		if tmp.GetUser() != name && tmp.GetWeight() != weights[index] {
			t.Errorf("Wrong user %s %d, expected %s %d", tmp.GetUser(), tmp.GetWeight(), name, weights[index])
		}
	}
}
