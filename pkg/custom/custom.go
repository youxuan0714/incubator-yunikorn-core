package custom

import (
	"github.com/apache/yunikorn-core/pkg/custom/fair"
	"github.com/apache/yunikorn-core/pkg/custom/lb"
)

var GlobalFairManager *fair.FairManager
var GlobalLBManager *lb.LBManager

func init() {
	GlobalFairManager = fair.NewFairManager()
	GlobalLBManager = lb.NewLBManager()
}

func GetFairManager() *fair.FairManager {
	return GlobalFairManager
}

func GetLBManager() *lb.LBManager {
	return GlobalLBManager
}
