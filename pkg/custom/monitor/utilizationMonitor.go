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
		}
		log.Logger().Info("utilization count", zap.Uint64("count", m.count))
		releaseTime := allocatedTime.Add(time.Second * time.Duration(req.Resources[sicommon.Duration]))
		d1 := SubTimeAndTranslateToUint64(allocatedTime, m.startTime)
		m.AddGlobalEventsTime(d1)
		d2 := SubTimeAndTranslateToUint64(releaseTime, m.startTime)
		m.AddGlobalEventsTime(d2)
		n.Allocate(d1, d2, req)
		m.count++
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
		m.file.SetCellValue(migsheet, fmt.Sprintf("%s%d", idLetter, 1), nodeID)
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
		m.file.SetCellValue(fairness, fmt.Sprintf("%s%d", timestampLetterOfUitlization, placeNum), timestamp)
		nodesRes := make([]*resources.Resource, 0)
		for nodeID, nodeRes := range m.nodes {
			_ = nodeRes.AllocateResource(timestamp)
			cap := nodeRes.cap.Clone()
			allocated := resources.Sub(nodeRes.cap.Clone(), nodeRes.avaialble.Clone())
			log.Logger().Info("uitilization trace", zap.Any("cap", cap), zap.Any("allocated", allocated))
			utilization := resources.CalculateAbsUsedCapacity(cap, allocated)
			nodesRes = append(nodesRes, utilization.Clone())
			// mig
			idLetter := m.id[nodeID]
			m.file.SetCellValue(migsheet, fmt.Sprintf("%s%d", idLetter, placeNum), int64(resources.MIG(utilization)))
		}
		average := resources.Average(nodesRes)
		gapSum := resources.NewResource()
		// sum += (utilization - average utilization)^2
		for _, n := range nodesRes {
			gap := resources.Sub(n, average)
			powerGap := resources.Power(gap, float64(2))
			gapSum = resources.Add(gapSum, powerGap)
		}
		// Max deviation = Max(SQRT(sum including cpu and memory))
		gapSum = resources.Power(gapSum, float64(0.5))
		standardDeviation := resources.Max(gapSum)
		m.file.SetCellValue(migsheet, fmt.Sprintf("%s%d", bias, placeNum), int64(standardDeviation))
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
		n.events[allocated] = resources.Sub(nil, reqWithoutDuration.Clone())
	} else {
		n.events[allocated] = resources.Sub(n.events[allocated], reqWithoutDuration.Clone())
	}

	if _, ok := n.events[release]; !ok {
		n.events[release] = reqWithoutDuration.Clone()
	} else {
		n.events[release] = resources.Add(n.events[allocated], reqWithoutDuration.Clone())
	}
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
