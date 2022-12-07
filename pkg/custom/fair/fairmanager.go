package fair

import (
	"container/heap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/users"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
)

type FairManager struct {
	tenants *urm.UserResourceManager
	apps    map[string]*apps.AppsHeap
}

func (f *FairManager) GetTenants() *urm.UserResourceManager {
	return f.tenants
}

func NewFairManager() *FairManager {
	return &FairManager{
		tenants: urm.NewURM(),
		apps:    make(map[string]*apps.AppsHeap, 0),
	}
}

func (f *FairManager) ContinueSchedule() bool {
	minUser := f.GetTenants().GetMinResourceUser()
	if h, ok := f.apps[minUser]; !ok {
		f.apps[minUser] = apps.NewAppsHeap()
		return false
	} else {
		if h.Len() == 0 {
			return false
		}
	}
	return true
}

func (f *FairManager) ParseUsersInPartitionConfig(conf configs.PartitionConfig) {
	records := f.GetTenants()
	for _, q := range conf.Queues {
		acl, _ := security.NewACL(q.SubmitACL)
		for user, _ := range acl.GetUsers() {
			records.AddUser(user)
		}
	}
}

func (f *FairManager) ParseUserInApp(app *objects.Application) {
	user := app.GetUser().User
	f.GetTenants().AddUser(user)
	if _, ok := f.apps[user]; !ok {
		f.apps[user] = apps.NewAppsHeap()
	}

	h := f.apps[user]
	heap.Push(h, apps.NewAppInfo(app.ApplicationID, app.SubmissionTime))
}

func (f *FairManager) NextAppToSchedule() (string, string) {
	user := f.GetTenants().GetMinResourceUser()
	h := f.apps[user]
	target := heap.Pop(h).(*apps.AppInfo)
	heap.Push(h, target)
	return user, target.ApplicationID
}

func (f *FairManager) UpdateScheduledApp(user string, resources map[string]int64, duration uint64) {
	heap.Pop(f.apps[user])
	resources["Duration"] = int64(duration)
	f.GetTenants().UpdateUser(users.NewScoreInfo(user, resources))
}
