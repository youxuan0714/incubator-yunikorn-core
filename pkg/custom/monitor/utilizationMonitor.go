package monitor

import (
	"fmt"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/custom/util"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"
	"os"
	"sort"
	"time"

	excel "github.com/xuri/excelize/v2"
)

type NodeUtilizationMonitor struct {
	nodes             map[string]*NodeResource
	id                map[string]string
	GlobalEventUnique map[uint64]bool
	GlobalEvent       []uint64
	startTime         time.Time
	file              *excel.File
	count             uint64
	First             bool
}

func NewUtilizationMonitor() *NodeUtilizationMonitor {
	file := excel.NewFile()
	file.NewSheet(migsheet)
	file.NewSheet(deviationsheet)
	file.SetCellValue(deviationsheet, "B1", "deviation")
	return &NodeUtilizationMonitor{
		nodes:             make(map[string]*NodeResource),
		GlobalEventUnique: make(map[uint64]bool),
		GlobalEvent:       make([]uint64, 0),
		id:                make(map[string]string),
		file:              file,
		startTime:         time.Now(),
		count:             uint64(0),
		First:             false,
	}
}

func (m *NodeUtilizationMonitor) SetStartTime(t time.Time) {
	m.startTime = t
}

func (m *NodeUtilizationMonitor) Allocate(nodeID string, allocatedTime time.Time, req *resources.Resource) {
	if n, ok := m.nodes[nodeID]; ok {
		if !m.First {
			m.startTime = allocatedTime
			m.First = true
			log.Logger().Info("utilization start time", zap.Any("utilization starttime", m.startTime))
		}
		releaseTime := allocatedTime.Add(time.Second * time.Duration(req.Resources[sicommon.Duration]))
		d1 := SubTimeAndTranslateToSeoncd(allocatedTime, m.startTime)
		d2 := SubTimeAndTranslateToSeoncd(releaseTime, m.startTime)
		log.Logger().Info("utilization count", zap.Uint64("count", m.count), zap.Uint64("allocate uint", d1), zap.Uint64("release uint", d2))

		m.AddGlobalEventsTime(d1)
		m.AddGlobalEventsTime(d2)

		n.Allocate(d1, d2, req)
		m.count++
		log.Logger().Info("save file: utilization", zap.Uint64("count", m.count))
		if m.count == appNum {
			m.Save()
		}
	}
}

func (m *NodeUtilizationMonitor) AddNode(n *objects.Node) {
	nodeID, avial, cap := util.ParseNode(n)
	if _, ok := m.nodes[nodeID]; !ok {
		m.id[nodeID] = excelColForUtilization[len(m.nodes)]
		m.nodes[nodeID] = NewNodeResource(avial, cap)
		idLetter := m.id[nodeID]
		cellName := fmt.Sprintf("%s%d", idLetter, 1)
		log.Logger().Info("node get id", zap.String("nodeID", idLetter), zap.String("cellName", cellName))
		m.file.SetCellValue(migsheet, cellName, nodeID)
	}
}

func (m *NodeUtilizationMonitor) AddGlobalEventsTime(t uint64) {
	if _, ok := m.GlobalEventUnique[t]; !ok {
		m.GlobalEventUnique[t] = true
		m.GlobalEvent = append(m.GlobalEvent, t)
	}
}

func (m *NodeUtilizationMonitor) Save() {
	DeleteExistedFile(utilizationfiltpath)
	sort.Slice(m.GlobalEvent, func(i, j int) bool { return m.GlobalEvent[i] < m.GlobalEvent[j] })
	for index, timestamp := range m.GlobalEvent {
		placeNum := uint64(index + 2)
		timestampCellName := fmt.Sprintf("%s%d", TimeStampLetter, placeNum)
		log.Logger().Info("timestamp cell info", zap.String("timestampCellName", timestampCellName), zap.Uint64("timestamp", timestamp))
		m.file.SetCellValue(migsheet, timestampCellName, timestamp)
		m.file.SetCellValue(deviationsheet, timestampCellName, timestamp)
		nodesRes := make([]*resources.Resource, 0)
		for nodeID, nodeRes := range m.nodes {
			_ = nodeRes.AllocateResource(timestamp)
			cap := nodeRes.cap.Clone()
			allocated := resources.Sub(nodeRes.cap.Clone(), nodeRes.avaialble.Clone())
			utilization := resources.CalculateAbsUsedCapacity(cap, allocated)
			log.Logger().Info("uitilization trace", zap.String("utilization", utilization.String()), zap.String("cap", cap.String()), zap.String("allocated", allocated.String()))
			nodesRes = append(nodesRes, utilization.Clone())
			// mig
			idLetter := m.id[nodeID]
			cellName := fmt.Sprintf("%s%d", idLetter, placeNum)
			mig := int64(resources.MIG(utilization))
			log.Logger().Info("mig", zap.String("celName", cellName), zap.Int64("mig", mig))
			m.file.SetCellValue(migsheet, cellName, mig)
		}
		average := resources.Average(nodesRes)
		standardDeviation := resources.GetDeviationFromNodes(nodesRes, average)
		cellName := fmt.Sprintf("%s%d", deviationCellName, placeNum)
		log.Logger().Info("deviation", zap.String("cellName", cellName), zap.Float64("deviation", standardDeviation))
		m.file.SetCellValue(deviationsheet, cellName, standardDeviation)
	}
	_ = os.Remove(utilizationfiltpath)
	if err := m.file.SaveAs(utilizationfiltpath); err != nil {
		log.Logger().Warn("save utilzation file fail", zap.String("err", err.Error()))
	} else {
		log.Logger().Info("uitilization file saved!")
	}
}

type NodeResource struct {
	avaialble, cap *resources.Resource
	events         map[uint64]*resources.Resource
}

func NewNodeResource(avaialble, cap *resources.Resource) *NodeResource {
	return &NodeResource{
		avaialble: avaialble.Clone(),
		cap:       cap.Clone(),
		events:    make(map[uint64]*resources.Resource),
	}
}

func (n *NodeResource) Allocate(allocated, release uint64, req *resources.Resource) {
	reqWithoutDuration := req.Clone()
	delete(reqWithoutDuration.Resources, sicommon.Duration)
	if _, ok := n.events[allocated]; !ok {
		n.events[allocated] = resources.Sub(resources.NewResource(), reqWithoutDuration.Clone())
	} else {
		n.events[allocated] = resources.Sub(n.events[allocated], reqWithoutDuration.Clone())
	}
	log.Logger().Info("Allocate request map", zap.String("allocated", n.events[allocated].String()), zap.String("request", req.String()))

	if _, ok := n.events[release]; !ok {
		n.events[release] = reqWithoutDuration.Clone()
	} else {
		n.events[release] = resources.Add(n.events[release], reqWithoutDuration.Clone())
	}
	log.Logger().Info("release request map", zap.String("released", n.events[allocated].String()), zap.String("release", req.String()))

	return
}

func (n *NodeResource) AllocateResource(timestamp uint64) bool {
	if value, ok := n.events[timestamp]; !ok {
		return false
	} else {
		n.avaialble = resources.Add(n.avaialble, value)
	}
	return true
}
