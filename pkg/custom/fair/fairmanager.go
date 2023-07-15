package fair

import (
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

type FairManager struct {
	tenants         *urm.UserResourceManager
	unscheduledApps map[string]*apps.AppsHeap
	scheduledApps   map[string]bool

	nodesID         map[string]*resouces.Resource
	clusterResource *resources.Resource
}

func (f *FairManager) GetTenants() *urm.UserResourceManager {
	return f.tenants
}

func NewFairManager() *FairManager {
	return &FairManager{
		tenants:         urm.NewURM(),
		unscheduledApps: make(map[string]*apps.AppsHeap, 0),
		scheduledApps:   make(map[string]bool, 0),
		nodesID:         make(map[string]*resources.Resource, 0),
		clusterResource: resources.NewResource()
	}
}
