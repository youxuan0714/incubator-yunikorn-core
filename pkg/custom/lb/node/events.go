package node

type Events []*Event

func (h Events) Len() int { return len(h) }
func (h Events) Less(i, j int) bool {
	if h[i].Timestamp.Equal(h[j].Timestamp) {
		return h[i].AppID < h[j].AppID
	}
	return h[i].Timestamp.Before(h[j].Timestamp)
}
func (h Events) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *Events) Push(x interface{}) {
	*h = append(*h, x.(*Event))
}

func (h *Events) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func NewEvents() *Events {
	tmp := make(Events, 0)
	return &tmp
}
