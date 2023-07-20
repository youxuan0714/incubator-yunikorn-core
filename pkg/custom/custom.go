package custom

import (
	"github.com/apache/yunikorn-core/pkg/custom/fair"
	"github.com/apache/yunikorn-core/pkg/custom/lb"
	"github.com/apache/yunikorn-core/pkg/custom/monitor"
)

var GlobalFairManager *fair.FairManager
var GlobalLBManager *lb.LBManager
var GlobalFairnessMonitor *monitor.FairnessMonitor
var GlobalNodeUtilizationMonitor *monitor.NodeUtilizationMonitor

func init() {
	GlobalFairManager = fair.NewFairManager()
	GlobalLBManager = lb.NewLBManager()
	GlobalFairnessMonitor = monitor.NewFairnessMonitor()
	GlobalNodeUtilizationMonitor = monitor.NewUtilizationMonitor()
	GlobalFairManager.GetDRFs = GlobalFairnessMonitor.UpdateCompletedApp
}

func GetFairManager() *fair.FairManager {
	return GlobalFairManager
}

func GetLBManager() *lb.LBManager {
	return GlobalLBManager
}

func GetFairMonitor() *monitor.FairnessMonitor {
	return GlobalFairnessMonitor
}

func GetNodeUtilizationMonitor() *monitor.NodeUtilizationMonitor {
	return GlobalNodeUtilizationMonitor
}
