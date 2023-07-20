package urm

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"sync"
)

type userApps struct {
	apps          map[string]*resources.Resource
	CompletedApps map[string]bool

	sync.RWMutex
}

func NewUserApps() *userApps {
	return &userApps{
		apps:          make(map[string]*resources.Resource, 0),
		CompletedApps: make(map[string]bool),
	}
}

func (u *userApps) RunApp(appID string, res *resources.Resource) {
	u.Lock()
	defer u.Unlock()
	if val, ok := u.apps[appID]; ok {
		if !resources.StrictlyGreaterThanOrEquals(val, res) {
			u.apps[appID] = res.Clone()
		}
	} else {
		u.apps[appID] = res.Clone()
	}
}

func (u *userApps) CompeleteApp(appID string) {
	u.Lock()
	defer u.Unlock()
	if _, ok := u.apps[appID]; ok {
		delete(u.apps, appID)
		u.CompletedApps[appID] = true
	} else {
		u.CompletedApps[appID] = false
	}
}

func (u *userApps) ComputeGlobalDominantResource(clusterResource *resources.Resource) float64 {
	u.Lock()
	defer u.Unlock()
	for appID, del := range u.CompletedApps {
		if !del {
			if _, exist := u.apps[appID]; exist {
				u.CompeleteApp(appID)
			}
		}
	}
	apps := make([]*resources.Resource, 0)
	for _, app := range u.apps {
		apps = append(apps, app.Clone())
	}
	return resources.ComputGlobalDominantResource(apps, clusterResource.Clone())
}
