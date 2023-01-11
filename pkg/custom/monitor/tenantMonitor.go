package monitor

import (
	"strconv"

	"fmt"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	customutil "github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	excel "github.com/xuri/excelize/v2"
	"go.uber.org/zap"
	"os"
	"sort"
	"time"
)

type FairnessMonitor struct {
	UnRunningApps           map[string]*objects.Application
	MasterResourceOfTenants map[string]uint64
	id                      map[string]string               // tenant id in excel A, B, C
	file                    *excel.File                     // excel
	Infos                   map[string]*MasterResourceInfos // tenant -> event
	eventsTimestampsUnique  map[uint64]bool
	eventsTimestamps        []uint64
	startTime               time.Time
	count                   uint64
	First                   bool
}

// Initialize the tenant Monitor
func NewFairnessMonitor() *FairnessMonitor {
	file := excel.NewFile()
	file.NewSheet(fairness)
	return &FairnessMonitor{
		UnRunningApps:           make(map[string]*objects.Application, 0),
		MasterResourceOfTenants: make(map[string]uint64, 0),
		id:                      make(map[string]string),
		file:                    file,
		eventsTimestampsUnique:  make(map[uint64]bool),
		eventsTimestamps:        make([]uint64, 0),
		Infos:                   make(map[string]*MasterResourceInfos),
		count:                   uint64(0),
		First:                   false,
	}
}

// Add application referrence when there is new application
func (m *FairnessMonitor) RecordUnScheduledApp(app *objects.Application) {
	if _, ok := m.UnRunningApps[app.ApplicationID]; !ok {
		m.UnRunningApps[app.ApplicationID] = app
		_, username, _ := customutil.ParseApp(app)
		if _, ok := m.id[username]; !ok {
			m.id[username] = excelCol[len(m.MasterResourceOfTenants)]
			m.MasterResourceOfTenants[username] = uint64(0)
			// write tenant id in B1, C1, D1 ...
			idLetter := m.id[username]
			log.Logger().Info("dynamic set tenant ID", zap.String("tenant name", username), zap.String("tenant ID", idLetter), zap.String("next idLetter", excelCol[len(m.MasterResourceOfTenants)]))
			m.file.SetCellValue(fairness, fmt.Sprintf("%s%d", idLetter, 1), username)
		}
	}
}

// this function would be called when application is in running status
func (m *FairnessMonitor) UpdateTheTenantMasterResource(app *objects.Application) {
	appID := app.ApplicationID
	if _, ok := m.UnRunningApps[appID]; !ok {
		log.Logger().Info("fairness unrecord app", zap.String("app", appID))
		return
	}

	user := app.GetUser().User
	masterResource := CalculateMasterResourceOfApplication(app)
	// events: global
	currentTime := time.Now()
	if !m.First {
		m.startTime = currentTime
		m.First = true
	}
	duration := SubTimeAndTranslateToMiliSecond(currentTime, m.startTime)
	log.Logger().Info("Add duration to excel", zap.Uint64("duration", duration))
	m.AddEventTimeStamp(duration)
	m.count++
	if m.count == appNum {
		m.Save()
	}

	// events: person
	if _, ok := m.Infos[user]; !ok {
		m.Infos[user] = NewMasterResourceInfos()
	}
	h := m.Infos[user]
	h.AddInfo(NewAddMasterResourceInfo(user, duration, masterResource))
	// stream
	m.AddMasterResourceToTenant(user, masterResource)
	log.Logger().Info("fairness print", zap.Any("apps", app.ApplicationID), zap.Any("tenants", m.MasterResourceOfTenants))
	return
}

// Add master resource to specific tenant
func (m *FairnessMonitor) AddMasterResourceToTenant(user string, masterResource uint64) {
	log.Logger().Warn("Update master resource who is not paresd in config, add it", zap.String("user", user), zap.Uint64("masterResource", masterResource))
	if _, ok := m.MasterResourceOfTenants[user]; !ok {
		m.MasterResourceOfTenants[user] = masterResource
	} else {
		m.MasterResourceOfTenants[user] += masterResource
	}
}

// Analyze the partition config and get the tenants
func (m *FairnessMonitor) ParseTenantsInPartitionConfig(conf configs.PartitionConfig) {
	users := customutil.ParseUsersInPartitionConfig(conf)
	for userNameInConfig, _ := range users {
		if _, ok := m.MasterResourceOfTenants[userNameInConfig]; !ok {
			// 1. update excel id, excel id From A,B,C -> 0, 1, 2
			// 2. add master resource event of users
			// 3. update stream master resource
			m.id[userNameInConfig] = excelCol[len(m.MasterResourceOfTenants)]
			m.MasterResourceOfTenants[userNameInConfig] = uint64(0)
			// write tenant id in B1, C1, D1 ...
			idLetter := m.id[userNameInConfig]
			log.Logger().Info("Set tenant ID", zap.String("tenant ID", idLetter), zap.String("next idLetter", excelCol[len(m.MasterResourceOfTenants)]))
			m.file.SetCellValue(fairness, fmt.Sprintf("%s%d", idLetter, 1), userNameInConfig)
		}
	}
}

// Save excel file
func (m *FairnessMonitor) Save() {
	var cellName string
	DeleteExistedFile(tenantsfiltpath)
	// setting timestamps
	// Write timestamps in A2,A3,A4...
	// If tenants has a related value, such as B3. When A3 is writed, B3 will be writed too.
	sort.Slice(m.eventsTimestamps, func(i, j int) bool { return m.eventsTimestamps[i] < m.eventsTimestamps[j] })
	currentMasterResource := make(map[string]uint64)
	for username, _ := range m.MasterResourceOfTenants {
		currentMasterResource[username] = uint64(0)
	}

	log.Logger().Info("Save file", zap.Int("Number of eventsTimesstamps", len(m.eventsTimestamps)))
	for index, timestamp := range m.eventsTimestamps {
		// A is timestamp.
		// B,C,D and so on is tenant master resource
		placeNum := uint64(index + 2)
		cellName = fmt.Sprintf("%s%d", TimeStampLetter, placeNum)
		log.Logger().Info("specific timestamp", zap.String("cellName", cellName), zap.Uint64("timestamp", timestamp))
		m.file.SetCellValue(fairness, cellName, timestamp)
		for username, events := range m.Infos {
			idLetter := m.id[username]
			cellName = fmt.Sprintf("%s%d", idLetter, placeNum)
			if masterResource, existed := events.MasterResourceAtTime(timestamp); existed {
				currentMasterResource[username] += masterResource
				log.Logger().Info("add master resource in tenants trace", zap.String("tenant", username), zap.Uint64("new master resource", masterResource))
			}
			masterResource := currentMasterResource[username]
			log.Logger().Info("master resource of specific timestamp", zap.Uint64("timestamp", timestamp), zap.String("tenant", username), zap.String("cellName", cellName), zap.Uint64("master resource", masterResource))
			m.file.SetCellValue(fairness, cellName, masterResource)
		}
	}
	_ = os.Remove(tenantsfiltpath)
	if err := m.file.SaveAs(tenantsfiltpath); err != nil {
		log.Logger().Warn("save tenants file fail", zap.String("err", err.Error()))
	} else {
		log.Logger().Info("save tenant file sucess")
	}
}

func (m *FairnessMonitor) AddEventTimeStamp(timestamp uint64) {
	if _, ok := m.eventsTimestampsUnique[timestamp]; ok {
		return
	}
	m.eventsTimestampsUnique[timestamp] = true
	m.eventsTimestamps = append(m.eventsTimestamps, timestamp)
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

type AddMasterResourceInfo struct {
	TenantID       string
	TimeStamp      uint64
	MasterResource uint64
}

func NewAddMasterResourceInfo(id string, d, masterResource uint64) *AddMasterResourceInfo {
	return &AddMasterResourceInfo{
		TenantID:       id,
		TimeStamp:      d,
		MasterResource: masterResource,
	}
}

type MasterResourceInfos struct {
	timestamps map[uint64]uint64
}

func NewMasterResourceInfos() *MasterResourceInfos {
	return &MasterResourceInfos{
		timestamps: make(map[uint64]uint64),
	}
}

func (m *MasterResourceInfos) AddInfo(a *AddMasterResourceInfo) {
	if _, ok := m.timestamps[a.TimeStamp]; !ok {
		m.timestamps[a.TimeStamp] = a.MasterResource
	} else {
		m.timestamps[a.TimeStamp] += a.MasterResource
	}
}

func (m *MasterResourceInfos) MasterResourceAtTime(timestamp uint64) (uint64, bool) {
	if value, ok := m.timestamps[timestamp]; !ok {
		return 0, false
	} else {
		return value, true
	}
}
