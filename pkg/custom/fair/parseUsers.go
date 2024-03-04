package fair

import (
	"container/heap"
	"strconv"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
	"github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"

	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

// Add the names of users in the config to the fairmanager
func (f *FairManager) ParseUsersInPartitionConfig(conf configs.PartitionConfig) {
	records := f.GetTenants()
	users := util.ParseUsersInPartitionConfig(conf)
	for user, _ := range users {
		records.AddUser(user)
	}
}

// If there is a new tenant's name in the new submitted application, add the username to the fairmanager
func (f *FairManager) ParseUserInApp(input *objects.Application) {
	appID, user, res := util.ParseApp(input)
	f.GetTenants().AddUser(user)
	if _, ok := f.unscheduledApps[user]; !ok {
		f.unscheduledApps[user] = apps.NewAppsHeap()
	}
	duration := strconv.FormatInt(int64(res.Resources["duration"]),10)
	h := f.unscheduledApps[user]
	auh := f.allUnscheduledApps
	info := apps.NewAppInfo(appID, input.SubmissionTime, duration)
	heap.Push(h, info)
	heap.Push(auh, info)
	log.Logger().Info("Add application in fair manager", zap.String("user", user), zap.String("applicationID", appID), zap.String("Duration", duration), zap.Int("heap", h.Len()))
}
