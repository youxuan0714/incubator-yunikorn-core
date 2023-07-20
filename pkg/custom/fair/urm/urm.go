package urm

import (
	"container/heap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/users"
	"github.com/apache/yunikorn-core/pkg/log"
	// "go.uber.org/zap"
)

type UserResourceManager struct {
	existedUser map[string]*userApps
	priority    *users.UsersHeap
	DRF         map[string]float64
}

func NewURM() *UserResourceManager {
	return &UserResourceManager{
		existedUser: make(map[string]*userApps, 0),
		priority:    users.NewUserHeap(),
		DRF:         make(map[string]float64),
	}
}

func (u *UserResourceManager) AddUser(name string) {
	if _, ok := u.existedUser[name]; !ok {
		u.existedUser[name] = NewUserApps()
	}
}

func (u *UserResourceManager) GetMinResourceUser(apps map[string]*apps.AppsHeap, clusterResource *resources.Resource) string {
	clusterRes := clusterResource.Clone()
	for userName, apps := range u.existedUser {
		drf := apps.ComputeGlobalDominantResource(clusterRes)
		u.DRF[userName] = drf
		heap.Push(u.priority, users.NewScore(userName, drf))
	}

	if u.priority.Len() == 0 {
		log.Logger().Warn("userheap should not be empty when getting min")
		return ""
	}

	// return the user with min resource if this user has unscheduled apps
	var s *users.Score
	for u.priority.Len() > 0 {
		tmp := heap.Pop(u.priority).(*users.Score)
		if requests, ok := apps[tmp.GetUser()]; ok {
			if requests.Len() > 0 {
				s = tmp
				break
			}
		}
	}

	for u.priority.Len() > 0 {
		heap.Pop(u.priority)
	}

	if s == nil {
		return ""
	}
	//log.Logger().Info("DRF", zap.String("user", s.GetUser()), zap.Any("users", u.DRF))
	return s.GetUser()
}

func (u *UserResourceManager) Allocate(user string, appID string, res *resources.Resource) {
	u.existedUser[user].RunApp(appID, res)
}

func (u *UserResourceManager) Release(user string, appID string) {
	if apps, ok := u.existedUser[user]; ok {
		apps.CompeleteApp(appID)
		apps.CompletedApps[appID] = false
	}
}

func (u *UserResourceManager) GetDRFs(cluster *resources.Resource) map[string]float64 {
	result := make(map[string]float64, 0)
	for userName, apps := range u.existedUser {
		drf := apps.ComputeGlobalDominantResource(cluster)
		if drf >= 1 {
			log.Logger().Info("DRF", zap.String("user", userName), zap.Any("users", u.DRF))
		}
		u.DRF[userName] = drf
	}
	return result
}
