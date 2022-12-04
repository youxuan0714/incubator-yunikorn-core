package urm

import (
	"container/heap"
	"errors"

	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/users"
)

type UserResourceManager struct {
	existedUser map[string]*users.Score
	priority    *users.UsersHeap
}

func NewURM() *UserResourceManager {
	return &UserResourceManager{
		existedUser: make(map[string]*users.Score, 0),
		priority:    users.NewUserHeap(),
	}
}

func (u *UserResourceManager) AddUser(name string) {
	if _, ok := u.existedUser[name]; !ok {
		s := users.NewScore(name, 0)
		u.existedUser[name] = s
		heap.Push(u.priority, s)
	}
}

func (u *UserResourceManager) GetMinResourceUser() string {
	s := heap.Pop(u.priority).(*users.Score)
	heap.Push(u.priority, s)
	return s.GetUser()
}

func (u *UserResourceManager) UpdateMinUser(info *users.ScoreInfo) error {
	s := heap.Pop(u.priority).(*users.Score)
	if info.GetUser() != s.GetUser() {
		heap.Push(u.priority, s)
		return errors.New("user is not same")
	}
	s.SumWeight(info.GetResources())
	heap.Push(u.priority, s)
	return nil
}
