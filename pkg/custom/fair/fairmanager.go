package fair

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
	"sync"
)

type FairManager struct {
	tenants         *urm.UserResourceManager
	unscheduledApps map[string]*apps.AppsHeap
	scheduledApps   map[string]bool

	runningApps map[string]appInfo

	nodesID         map[string]*resources.Resource
	clusterResource *resources.Resource

	sync.RWMutex
}

type appInfo struct {
	user string
	res  *resources.Resource
}

func NewAppInfo(user string, res *resources.Resource) appInfo {
	return appInfo{
		user: user,
		res:  res.Clone(),
	}
}

func (f *FairManager) GetTenants() *urm.UserResourceManager {
	return f.tenants
}

func (f *FairManager) GetDRFs() map[string]float64 {
	return f.GetTenants().DRF
}

func (f *FairManager) GetClusterResource() *resources.Resource {
	return f.clusterResource.CLone()
}

func NewFairManager() *FairManager {
	return &FairManager{
		tenants:         urm.NewURM(),
		unscheduledApps: make(map[string]*apps.AppsHeap, 0),
		scheduledApps:   make(map[string]bool, 0),
		runningApps:     make(map[string]appInfo, 0),
		nodesID:         make(map[string]*resources.Resource, 0),
		clusterResource: resources.NewResource(),
	}
}
