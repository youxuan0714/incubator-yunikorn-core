package fair

import (
	"container/heap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	customutil "github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"

	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

// Add the names of users in the config to the fairmanager
func (f *FairManager) ParseUsersInPartitionConfig(conf configs.PartitionConfig) {
	records := f.GetTenants()
	users := customutil.ParseUsersInPartitionConfig(conf)
	for user, _ := range users {
		records.AddUser(user)
	}
}

// If there is a new tenant's name in the new submitted application, add the username to the fairmanager
func (f *FairManager) ParseUserInApp(input *objects.Application) {
	appID, user, _ := customutil.ParseApp(input)
	f.GetTenants().AddUser(user)
	if _, ok := f.apps[user]; !ok {
		f.apps[user] = apps.NewAppsHeap()
	}

	h := f.apps[user]
	info := apps.NewAppInfo(appID, input.SubmissionTime)
	heap.Push(h, info)
	log.Logger().Info("Add application in fair manager", zap.String("user", user), zap.String("applicationID", appID), zap.Int("heap", h.Len()))
}
