package urm

import (
	"container/heap"
	"errors"
	"fmt"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/users"
	"github.com/apache/yunikorn-core/pkg/log"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
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

func (u *UserResourceManager) GetMinResourceUser() string {
	if u.priority.Len() == 0 {
		log.Logger().Warn("userheap should not be empty when getting min")
	}
	s := heap.Pop(u.priority).(*users.Score)
	heap.Push(u.priority, s)
	return s.GetUser()
}

func (u *UserResourceManager) UpdateUser(user string, info *resources.Resource) error {
	if u.priority.Len() == 0 {
		return errors.New("userheap should not be empty when update min")
	}

	s := heap.Pop(u.priority).(*users.Score)
	if user != s.GetUser() {
		heap.Push(u.priority, s)
		return errors.New(fmt.Sprintf("score is %s, info is %s", s.GetUser(), user))
	}
	masterResource := resources.MasterResource(info)
	s.AddWeight(int64(masterResource.Resources[sicommon.Master]))
	heap.Push(u.priority, s)
	return nil
}
