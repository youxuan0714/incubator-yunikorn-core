package monitor

import (
	"fmt"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"
	"os"

	excel "github.com/xuri/excelize/v2"
)

const (
	utilizationfiltpath = "/tmp/utiliztion.xlsx"
	cpusheet            = "cpu"
	memsheet            = "mem"
	migsheet            = "mig"
)

var excelCol []string = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

type NodeUtilizationMonitor struct {
	nodes map[string]*objects.Node
	id    map[string]string
	count uint64
	file  *excel.File
}

func NewUtilizationMonitor() *NodeUtilizationMonitor {
	file := excel.NewFile()
	file.NewSheet(cpusheet)
	file.NewSheet(memsheet)
	file.NewSheet(migsheet)
	return &NodeUtilizationMonitor{
		nodes: make(map[string]*objects.Node, 0),
		id:    make(map[string]string, 0),
		count: 2,
		file:  file,
	}
}

func (m *NodeUtilizationMonitor) TraceNodes() {
	tmpFile := m.file
	for nodeID, node := range m.nodes {
		utilizations := node.GetUtilizedResource()
		mig := resources.MIG(utilizations)
		idLetter := m.id[nodeID]
		tmpFile.SetCellValue(cpusheet, fmt.Sprintf("%s%d", idLetter, m.count), int64(utilizations.Resources[sicommon.CPU]))
		tmpFile.SetCellValue(memsheet, fmt.Sprintf("%s%d", idLetter, m.count), int64(utilizations.Resources[sicommon.Memory]))
		tmpFile.SetCellValue(migsheet, fmt.Sprintf("%s%d", idLetter, m.count), int64(mig))
		log.Logger().Info("Trace MIG", zap.String("nodeID", nodeID), zap.Any("mig value", mig))
	}
	m.count++
	if m.count%100 == 0 {
		_ = os.Remove(utilizationfiltpath)
		if err := m.file.SaveAs(utilizationfiltpath); err != nil {
			log.Logger().Info("utilization file save fail", zap.Any("error", err))
		}
	}
}

func (m *NodeUtilizationMonitor) AddNode(n *objects.Node) {
	nodeID := n.NodeID
	if _, ok := m.nodes[nodeID]; !ok {
		m.id[nodeID] = excelCol[len(m.nodes)]
		m.nodes[nodeID] = n
		tmpFile := m.file
		idLetter := m.id[nodeID]
		tmpFile.SetCellValue(cpusheet, fmt.Sprintf("%s%d", idLetter, 1), nodeID)
		tmpFile.SetCellValue(memsheet, fmt.Sprintf("%s%d", idLetter, 1), nodeID)
		tmpFile.SetCellValue(migsheet, fmt.Sprintf("%s%d", idLetter, 1), nodeID)
	}
}
