package urm

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

type userApps struct {
	apps          map[string]*resources.Resource
	CompletedApps map[string]bool
}

func NewUserApps() *userApps {
	return &userApps{
		apps:          make(map[string]*resources.Resource, 0),
		CompletedApps: make(map[string]bool),
	}
}

func (u *userApps) RunApp(appID string, res *resources.Resource) {
	if val, ok := u.apps[appID]; ok {
		if !resources.StrictlyGreaterThanOrEquals(val, res) {
			u.apps[appID] = res.Clone()
		}
	} else {
		u.apps[appID] = res.Clone()
	}
}

func (u *userApps) CompeleteApp(appID string) {
	delete(u.apps, appID)
}

func (u *userApps) ComputeGlobalDominantResource(clusterResource *resources.Resource) float64 {
	for appID, _ := range u.CompletedApps {
		delete(u.apps, appID)
	}

	apps := make([]*resources.Resource, 0)
	for _, app := range u.apps {
		apps = append(apps, app.Clone())
	}
	return resources.ComputGlobalDominantResource(apps, clusterResource)
}
