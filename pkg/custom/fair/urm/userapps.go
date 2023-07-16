package urm

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/util"
)

type userApps struct {
	apps map[string]*resources.Resource
}

func NewUserApps() *userApps {
	return &userApps{
		apps: make(map[string]*resources.Resource, 0),
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
	apps := make([]*resources.Resource, 0)
	for appID, app := range u.apps {
		apps = append(apps, app.Clone())
	}
	return resources.ComputGlobalDominantResource(apps, clusterResource)
}
