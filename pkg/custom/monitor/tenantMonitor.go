package monitor

import (
	// "strconv"

	"fmt"
	// "math"
	"os"
	"sort"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	customutil "github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	// sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	excel "github.com/xuri/excelize/v2"
	"go.uber.org/zap"
	"sync"
)

const (
	DATASTART = 2
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

	sync.RWMutex
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
// If there is a new tenant, set the tenant with a new cell name
func (m *FairnessMonitor) RecordUnScheduledApp(app *objects.Application) {
	if _, ok := m.UnRunningApps[app.ApplicationID]; !ok {
		m.UnRunningApps[app.ApplicationID] = app
		_, username, _ := customutil.ParseApp(app)
		if _, ok := m.id[username]; !ok {
			m.id[username] = excelCol[len(m.MasterResourceOfTenants)]
			m.MasterResourceOfTenants[username] = uint64(0)
			// write tenant id in B1, C1, D1 ...
			idLetter := m.id[username]
			m.file.SetCellValue(fairness, fmt.Sprintf("%s%d", idLetter, 1), username)
			// log.Logger().Info("dynamic set tenant ID", zap.String("tenant name", username), zap.String("tenant ID", idLetter), zap.String("next idLetter", excelCol[len(m.MasterResourceOfTenants)]))
		}
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
			m.file.SetCellValue(fairness, fmt.Sprintf("%s%d", idLetter, 1), userNameInConfig)
		}
	}
}

// this function would be called when application is in running status
func (m *FairnessMonitor) UpdateTheTenantMasterResource(currentTime time.Time, app *objects.Application, drfs func() map[string]float64, clusterResource *resources.Resource) {
	m.Lock()
	defer m.Unlock()
	appID := app.ApplicationID
	if _, ok := m.UnRunningApps[appID]; !ok {
		// log.Logger().Info("fairness unrecord app", zap.String("app", appID))
		return
	}

	if !m.First {
		m.startTime = currentTime
		m.First = true
	}
	// log.Logger().Info("Add duration to excel", zap.Uint64("duration", duration))
	m.AddInfo(drfs(), SubTimeAndTranslateToSeoncd(currentTime, m.startTime))

	m.count++
	if m.count == appNum {
		m.Save()
	}
	return
}

func (m *FairnessMonitor) UpdateCompletedApp(results map[string]float64) {
	m.AddInfo(results, SubTimeAndTranslateToSeoncd(time.Now(), m.startTime))
}

func (m *FairnessMonitor) AddInfo(results map[string]float64, duration uint64) {
	m.AddEventTimeStamp(duration)
	for userName, drf := range results {
		if _, ok := m.Infos[userName]; !ok {
			m.Infos[userName] = NewMasterResourceInfos()
		}
		m.Infos[userName].AddInfo(NewAddMasterResourceInfo(userName, duration, drf))
	}
}

// Save excel file
func (m *FairnessMonitor) Save() {
	DeleteExistedFile(tenantsfiltpath)
	sort.Slice(m.eventsTimestamps, func(i, j int) bool { return m.eventsTimestamps[i] < m.eventsTimestamps[j] })
	// setting timestamps
	// Write timestamps in A2,A3,A4...
	// If tenants has a related value, such as B3. When A3 is writed, B3 will be writed too.
	for index, timestamp := range m.eventsTimestamps {
		// A is timestamp.
		// B,C,D and so on is tenant master resource
		placeNum := uint64(index + DATASTART)
		m.file.SetCellValue(fairness, fmt.Sprintf("%s%d", TimeStampLetter, placeNum), timestamp)
		for username, events := range m.Infos {
			if drf, existed := events.MasterResourceAtTime(timestamp); existed {
				m.file.SetCellValue(fairness, fmt.Sprintf("%s%d", m.id[username], placeNum), drf)
			}
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

type AddMasterResourceInfo struct {
	TenantID  string
	TimeStamp uint64
	DRF       float64
}

func NewAddMasterResourceInfo(id string, duration uint64, drf float64) *AddMasterResourceInfo {
	return &AddMasterResourceInfo{
		TenantID:  id,
		TimeStamp: duration,
		DRF:       drf,
	}
}

type MasterResourceInfos struct {
	timestamps map[uint64]float64
}

func NewMasterResourceInfos() *MasterResourceInfos {
	return &MasterResourceInfos{
		timestamps: make(map[uint64]float64),
	}
}

func (m *MasterResourceInfos) AddInfo(a *AddMasterResourceInfo) {
	m.timestamps[a.TimeStamp] = a.DRF
}

func (m *MasterResourceInfos) MasterResourceAtTime(timestamp uint64) (float64, bool) {
	if value, ok := m.timestamps[timestamp]; !ok {
		return 0, false
	} else {
		return value, true
	}
}
