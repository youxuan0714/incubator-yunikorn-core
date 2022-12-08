package util

import (
	"strconv"

	"github.com/apache/yunikorn-core/pkg/custom/fair"
	"github.com/apache/yunikorn-core/pkg/custom/lb"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
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

func ParseNode(n *objects.Node) (string, map[string]int64) {
	res := n.GetAvailableResource().Resources
	resResult := make(map[string]int64, 0)
	for key, value := range res {
		resResult[key] = int64(value)
	}
	return n.NodeID, resResult
}

func ParseApp(a *objects.Application) (string, string, map[string]int64, uint64) {
	resResult := make(map[string]int64)
	resType := []string{sicommon.CPU, sicommon.Memory, "Duration"}
	var duration uint64
	for _, key := range resType {
		if key == "Duration" {
			if value, err := strconv.ParseUint(a.GetTag("Duration"), 10, 64); err != nil {
				log.Logger().Warn("Duration parsing fail", zap.String("error", err.Error()))
			} else {
				duration = value
			}
			continue
		}
		if value, err := strconv.ParseInt(a.GetTag(key), 10, 64); err != nil {
			log.Logger().Warn("Resource parsing fail", zap.String(key),  zap.String("error", err.Error()))
		} else {
			resResult[key] = value
		}
	}
	return a.ApplicationID, a.GetUser().User, resResult, duration
}
