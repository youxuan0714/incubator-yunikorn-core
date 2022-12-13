package util

import (
	"strconv"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"
)

func ParseNode(n *objects.Node) (string, map[string]int64) {
	res := n.GetAvailableResource().Resources
	resResult := make(map[string]int64, 0)
	for key, value := range res {
		resResult[key] = int64(value)
	}
	return n.NodeID, resResult
}

func ParseApp(a *objects.Application) (appID string, username string, resResult map[string]int64, duration uint64) {
	appID = a.ApplicationID
	username = a.GetUser().User
	resResult = make(map[string]int64, 0)
	resType := []string{sicommon.CPU, sicommon.Memory, sicommon.Duration}
	for _, key := range resType {
		if key == sicommon.Duration {
			if value, err := strconv.ParseUint(a.GetTag(sicommon.Duration), 10, 64); err != nil {
				log.Logger().Warn("Duration parsing fail", zap.String("error", err.Error()))
			} else {
				duration = value
			}
			continue
		}
		if value, err := strconv.ParseInt(a.GetTag(key), 10, 64); err != nil {
			log.Logger().Warn("Resource parsing fail", zap.String("key", key), zap.String("error", err.Error()))
		} else {
			resResult[key] = value
		}
	}
	return
}

func ParseUsersInPartitionConfig(conf configs.PartitionConfig) map[string]bool {
	records := map[string]bool{}
	for _, q := range conf.Queues {
		acl, err := security.NewACL(q.SubmitACL)
		if err != nil {
			log.Logger().Warn("Parsing ACL in fair manager is failed", zap.String("error", err.Error()))
		}
		for user, _ := range acl.GetUsers() {
			log.Logger().Info("User in config", zap.String("user", user))
			if _, ok := records[user]; !ok {
				records[user] = true
			}
		}
	}
	return records
}
