package custom

import (
	"github.com/apache/yunikorn-core/pkg/custom/fair"
	"github.com/apache/yunikorn-core/pkg/custom/lb"
	"github.com/apache/yunikorn-core/pkg/custom/monitor"
	"github.com/apache/yunikorn-core/pkg/custom/plan"
)

var GlobalFairManager *fair.FairManager
var GlobalLBManager *lb.LBManager
var GlobalFairnessMonitor *monitor.FairnessMonitor
var GlobalNodeUtilizationMonitor *monitor.NodeUtilizationMonitor
var GlobalPlanManager *plan.PlanManager

func init() {
	GlobalFairManager = fair.NewFairManager()
	GlobalLBManager = lb.NewLBManager()
	GlobalFairnessMonitor = monitor.NewFairnessMonitor()
	GlobalNodeUtilizationMonitor = monitor.NewUtilizationMonitor()
	GlobalPlanManager = plan.NewPlanManager()
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

func GetPlanManager() *plan.PlanManager {
	return GlobalPlanManager
}
