package users

type UsersHeap []*Score

func (h UsersHeap) Len() int           { return len(h) }
func (h UsersHeap) Less(i, j int) bool { return h[i].GetWeight() < h[j].GetWeight() }
func (h UsersHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *UsersHeap) Push(x interface{}) {
	*h = append(*h, x.(*Score))
}

func (h *UsersHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func NewUserHeap() *UsersHeap {
	h := make(UsersHeap, 0)
	return &h
}
