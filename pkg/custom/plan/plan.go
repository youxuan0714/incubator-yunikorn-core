package plan

type PlanManager struct {
	Scheduled       bool
	AppID           string
	StreamAppToNode string
	Nodes           map[string][]string
}

func NewPlanManager() *PlanManager {
	return &PlanManager{
		Nodes:           make(map[string][]string, 0),
		Scheduled:       false,
		AppID:           "",
		StreamAppToNode: "",
	}
}

func (p *PlanManager) AssignAppToNode(app string, nodeID string) {
	if _, ok := p.Nodes[nodeID]; !ok {
		p.Nodes[nodeID] = make([]string, 0)
	}
	p.Nodes[nodeID] = append(p.Nodes[nodeID], app)
}

func (p *PlanManager) GetNodes() map[string][]string {
	return p.Nodes
}

func (p *PlanManager) UpdateNodes(nodeID string, HandledApp int) {
	apps := p.Nodes[nodeID]
	apps = apps[HandledApp:]
	p.Nodes[nodeID] = apps
}

func (p *PlanManager) Clear(nodeID string) {
	p.Nodes[nodeID] = make([]string, 0)
}

func (p *PlanManager) Pop(nodeID string) {
	if tmp := p.Nodes[nodeID]; len(tmp) == 1 {
		p.Clear(nodeID)
	} else {
		p.Nodes[nodeID] = tmp[1:]
	}
}
