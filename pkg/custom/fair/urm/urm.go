package urm

import (
	"container/heap"
	"errors"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/users"
	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
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
		log.Logger().Info("Add user", zap.Int("user heap length", u.priority.Len()), zap.Int("user map length", len(u.existedUser)))
	}
}

func (u *UserResourceManager) GetMinResourceUser(apps map[string]*apps.AppsHeap) string {
	if u.priority.Len() == 0 {
		log.Logger().Warn("userheap should not be empty when getting min")
	}
	bk := make([]*Score, 0)
	var s *Score
	for u.priority.Len() > 0 {
		tmp := heap.Pop(u.priority).(*users.Score)
		bk = append(bk, tmp)
		if apps[s.GetUser].Len() > 0 {
			s = tmp
			break
		}
	}

	for _, element := range bk {
		heap.Push(u.priority, element)
	}
	return s.GetUser()
}

func (u *UserResourceManager) UpdateUser(user string, info *resources.Resource) error {
	if u.priority.Len() == 0 {
		return errors.New("userheap should not be empty when update min")
	}

	s := heap.Pop(u.priority).(*users.Score)
	/*
		if user != s.GetUser() {
			heap.Push(u.priority, s)
			return errors.New(fmt.Sprintf("score is %s, info is %s", s.GetUser(), user))
		}
	*/

	s.AddWeight(int64(resources.MasterResource(info)))
	log.Logger().Info("wieght", zap.String("user", s.GetUser()), zap.Int64("weight", s.GetWeight()))
	heap.Push(u.priority, s)
	return nil
}
