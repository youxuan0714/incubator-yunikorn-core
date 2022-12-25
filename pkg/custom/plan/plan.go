package plan

type PlanManager struct {
	Nodes map[string][]string
}

func NewPlanManager() *PlanManager {
	return &PlanManager{
		Nodes: make(map[string][]string, 0),
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

func (p *PlanManager) CompletedApps(nodeID string, HandledApp int) {
	if apps := p.Nodes[nodeID]; len(apps)-1 == HandledApp {
		p.Nodes[nodeID] = make([]string, 0)
	}
}
