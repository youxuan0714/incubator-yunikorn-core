package fair

import (
	"container/heap"

	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
)

func (f *FairManager) NextAppToSchedule() (bool, string, string) {
	user := f.GetTenants().GetMinResourceUser(f.apps)
	h, ok := f.apps[user]
	if !ok {
		//log.Logger().Info("Non existed user apps", zap.String("user", user))
		f.apps[user] = apps.NewAppsHeap()
		return false, "", ""
	}

	if h.Len() == 0 {
		//log.Logger().Info("User does not has apps", zap.String("user", user))
		return false, "", ""
	}

	target := heap.Pop(h).(*apps.AppInfo)
	if _, exist := f.waitToDelete[target.ApplicationID]; exist {
		delete(f.waitToDelete, target.ApplicationID)
		if h.Len() > 0 {
			target = heap.Pop(h).(*apps.AppInfo)
			heap.Push(h, target)
		} else {
			return false, "", ""
		}
	} else {
		heap.Push(h, target)
	}

	appID := target.ApplicationID
	//log.Logger().Info("User has apps", zap.String("user", user), zap.String("appid", target.ApplicationID), zap.Int("heap", h.Len()))
	return true, user, appID
}
