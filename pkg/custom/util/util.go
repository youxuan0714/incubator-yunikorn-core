package util

import (
	"strconv"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	sicommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"
)

// Parse the vcore and memory in node
func ParseNode(n *objects.Node) (nodeID string, avialble *resources.Resource, cap *resources.Resource) {
	nodeID = n.NodeID
	avialble = resources.NewResource()
	cap = resources.NewResource()
	resType := []string{sicommon.CPU, sicommon.Memory}

	res := n.GetAvailableResource().Resources
	for _, targetType := range resType {
		avialble.Resources[targetType] = res[targetType]
	}

	res = n.GetCapacity().Resources
	for _, targetType := range resType {
		cap.Resources[targetType] = res[targetType]
	}

	return
}

//
func ParseApp(a *objects.Application) (appID string, username string, resResult *resources.Resource) {
	appID = a.ApplicationID
	username = a.GetUser().User
	resResult = resources.NewResource()
	resType := []string{sicommon.CPU, sicommon.Memory, sicommon.Duration}
	for _, key := range resType {
		if value, err := strconv.ParseInt(a.GetTag(key), 10, 64); err != nil {
			log.Logger().Warn("Resource parsing fail", zap.String("key", key), zap.String("error", err.Error()))
		} else {
			resResult.Resources[key] = resources.Quantity(value)
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
