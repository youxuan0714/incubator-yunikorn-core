package monitor

import (
	"strconv"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	customutil "github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"
)

type FairnessMonitor struct {
	UnRunningApps           map[string]*objects.Application
	MasterResourceOfTenants map[string]uint64
}

var TenantMonitor *FairnessMonitor

// Initialize the tenant Monitor
func init() {
	TenantMonitor = &FairnessMonitor{
		UnRunningApps:           make(map[string]*objects.Application, 0),
		MasterResourceOfTenants: make(map[string]uint64, 0),
	}
}

// Add application referrence when there is new application
func (m *FairnessMonitor) RecordUnScheduledApp(app *objects.Application) {
	if _, ok := m.UnRunningApps[app.ApplicationID]; !ok {
		m.UnRunningApps[app.ApplicationID] = app
	}
}

// Add master resource to specific tenant
func (m *FairnessMonitor) AddMasterResourceToTenant(user string, masterResource uint64) {
	if _, ok := m.MasterResourceOfTenants[user]; !ok {
		m.MasterResourceOfTenants[user] = masterResource
		log.Logger().Warn("Update master resource who is not paresd in config, add it", zap.String("user", user), zap.Uint64("masterResource", masterResource))
	} else {
		m.MasterResourceOfTenants[user] += masterResource
	}
}

// this function would be called when application is in running status
func (m *FairnessMonitor) UpdateTheTenantMasterResource(app *objects.Application) {
	appID := app.ApplicationID
	if _, ok := m.UnRunningApps[appID]; !ok {
		// Already update this appliction to certain tenant, skip
		return
	}

	if running := app.IsRunning(); running {
		m.AddMasterResourceToTenant(app.GetUser().User, CalculateMasterResourceOfApplication(app))
	}
}

// Analyze the partition config and get the tenants
func (m *FairnessMonitor) ParseTenantsInPartitionConfig(conf configs.PartitionConfig) {
	users := customutil.ParseUsersInPartitionConfig(conf)
	for userNameInConfig, _ := range users {
		if _, ok := m.MasterResourceOfTenants[userNameInConfig]; !ok {
			m.MasterResourceOfTenants[userNameInConfig] = uint64(0)
		}
	}
}

// Print unscheduled apps and tenants' information
func (m *FairnessMonitor) FairnessLog() {
	log.Logger().Info("fairness print", zap.Any("apps", m.UnRunningApps), zap.Any("tenants", m.MasterResourceOfTenants))
}

// Calulate master resource of a application
func CalculateMasterResourceOfApplication(app *objects.Application) uint64 {
	var duration, cpu, memory uint64
	duration, err := strconv.ParseUint(app.GetTag(sicommon.Duration), 10, 64)
	if err != nil {
		log.Logger().Warn("tenant monitor fail parsing duration", zap.Any("err message", err))
	}
	cpu, err = strconv.ParseUint(app.GetTag(sicommon.CPU), 10, 64)
	if err != nil {
		log.Logger().Warn("tenant monitor fail parsing cpu", zap.Any("err message", err))
	}
	memory, err = strconv.ParseUint(app.GetTag(sicommon.Memory), 10, 64)
	if err != nil {
		log.Logger().Warn("tenant monitor fail parsing memory", zap.Any("err message", err))
	}
	return duration * cpu * memory
}
