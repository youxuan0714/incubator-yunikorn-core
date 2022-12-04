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

func NewFaireManager() *FairManager {
	return &FairManager{
		tenants: urm.NewURM(),
		apps:    make(map[string]*apps.AppsHeap, 0),
	}
}

func (f *FairManager) ParseUsersInPartitionConfig(conf configs.PartitionConfig) {
	q := make([]configs.QueueConfig, 0)
	q = append(q, conf.Queues...)
	records := f.GetTenants()
	for total := len(q); total > 0; {
		top := q[0]
		acl, _ := security.NewACL(top.SubmitACL)
		for user := range acl.GetUsers() {
			records.AddUser(user)
		}

		if len(top.Queues) > 0 {
			q = append(q, top.Queues...)
		}

		if len(q) > 1 {
			q = q[1:]
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

func (f *FairManager) UpdateScheduledApp(user string, resources map[string]int64) {
	heap.Pop(f.apps[user])
	f.GetTenants().UpdateMinUser(users.NewScoreInfo(user, resources))
}
