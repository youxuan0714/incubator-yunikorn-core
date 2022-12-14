package util

import (
	"testing"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestParseNode(t *testing.T) {
	nodeName := "test"
	expectedRes := map[string]int64{siCommon.CPU: 100, siCommon.Memory: 100}
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 100, siCommon.Memory: 100})
	proto := newProto(nodeName, totalRes, nil, map[string]string{})
	node := objects.NewNode(proto)
	nodeID, result := ParseNode(node)
	if nodeID != nodeName {
		t.Errorf("expected node name: %s, got %s", nodeName, nodeID)
	}
	if len(expectedRes) != len(result.Resources) {
		t.Errorf("expected len of res %d, got %d", len(expectedRes), len(result.Resources))
	} else {
		for key, value := range expectedRes {
			if got, ok := result.Resources[key]; !ok {
				t.Errorf("miss key %s", key)
			} else {
				tmp := int64(got)
				if value != tmp {
					t.Errorf("expected res %s: %d, got %d", key, value, tmp)
				}
			}
		}
	}
}

func TestParseApp(t *testing.T) {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	siApp := &si.AddApplicationRequest{
		ApplicationID:                "appID",
		QueueName:                    "some.queue",
		PartitionName:                "AnotherPartition",
		ExecutionTimeoutMilliSeconds: 0,
		Tags:                         map[string]string{siCommon.CPU: "200", siCommon.Memory: "300", siCommon.Duration: "100"},
		PlaceholderAsk:               &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 1}}},
	}
	app := objects.NewApplication(siApp, user, rmproxy.NewMockedRMProxy(), "myRM")
	id, username, res := ParseApp(app)
	if id != "appID" {
		t.Errorf("expected id is %s,got %s", "appID", id)
	}
	if username != "testuser" {
		t.Errorf("expected user is %s,got %s", "appID", username)
	}
	expected := map[string]int64{siCommon.CPU: 200, siCommon.Memory: 300, siCommon.Duration: 100}
	for key, value := range expected {
		if got, ok := res.Resources[key]; !ok {
			t.Errorf("missing tag %s", key)
		} else {
			if int64(got) != value {
				t.Errorf("tag %s expect %d, got %d", key, value, got)
			}
		}
	}
}

func newProto(nodeID string, totalResource, occupiedResource *resources.Resource, attributes map[string]string) *si.NodeInfo {
	proto := si.NodeInfo{
		NodeID:     nodeID,
		Attributes: attributes,
	}

	if totalResource != nil {
		proto.SchedulableResource = &si.Resource{
			Resources: map[string]*si.Quantity{},
		}
		for name, value := range totalResource.Resources {
			quantity := si.Quantity{Value: int64(value)}
			proto.SchedulableResource.Resources[name] = &quantity
		}
	}

	if occupiedResource != nil {
		proto.OccupiedResource = &si.Resource{
			Resources: map[string]*si.Quantity{},
		}
		for name, value := range occupiedResource.Resources {
			quantity := si.Quantity{Value: int64(value)}
			proto.OccupiedResource.Resources[name] = &quantity
		}
	}
	return &proto
}
