package fair

import (
	"container/heap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"go.uber.org/zap"
)

func (f *FairManager) UpdateScheduledApp(input *objects.Application) {
	appID, user, res := customutil.ParseApp(input)
	f.scheduledApps[appID] = true

	if h, ok := f.unscheduledApps[user]; !ok {
		log.Logger().Error("Non existed app update", zap.String("app", appID), zap.String("user", user))
	} else {
		log.Logger().Info("Update scheduled app", zap.Int("heap", h.Len()))
		bk := make([]*apps.AppInfo, 0)
		for h.Len() > 0 || len(f.scheduledApps) > 0 {
			target := heap.Pop(h).(*apps.AppInfo)
			id := target.ApplicationID
			if _, exist := f.scheduledApps[id]; !exist {
				log.Logger().Info("Delete app is not in the heap", zap.String("appid", id))
				bk = append(bk, target)
			} else {
				delete(f.scheduledApps, id)
				log.Logger().Info("Delete app", zap.String("appid", id), zap.Int("heap", h.Len()))
			}
		}

		for _, element := range bk {
			heap.Push(h, element)
		}
	}
	f.GetTenants().UpdateUser(user, res)
}

func (f *FairManager) AddNode(nodeID string, capicity *resrouce.Resource) {
	tmp := f.clusterResource.Clone()
	if cap, ok := f.nodesID[nodeID]; ok {
		if !resources.StrictlyGreaterThanOrEquals(cap, capacity) {
			tmp = resources.Sub(tmp, cap)
			tmp = resources.Add(tmp, capacity)
		}
	} else {
		tmp = resource.Add(tmp, capacity)
	}
	f.clusterResource = tmp
}

func (f *FairManager) RemoveNode(nodeID string) {
	if cap, ok := f.nodesID[nodeID]; ok {
		f.clusterResource = resources.Sub(f.clusterResource, cap)
		delete(f.nodesID, nodeID)
	}
}
