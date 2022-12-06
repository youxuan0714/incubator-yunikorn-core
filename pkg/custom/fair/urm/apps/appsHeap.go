package apps

import (
	"time"
)

type AppInfo struct {
	ApplicationID  string
	SubmissionTime time.Time
}

func NewAppInfo(id string, t time.Time) *AppInfo {
	return &AppInfo{
		ApplicationID:  id,
		SubmissionTime: t,
	}
}

type AppsHeap []*AppInfo

func (h AppsHeap) Len() int { return len(h) }
func (h AppsHeap) Less(i, j int) bool {
	if h[i].SubmissionTime.Equal(h[j].SubmissionTime) {
		return h[i].ApplicationID < h[j].ApplicationID
	}
	return h[i].SubmissionTime.Before(h[j].SubmissionTime)
}
func (h AppsHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *AppsHeap) Push(x interface{}) {
	*h = append(*h, x.(*AppInfo))
}

func (h *AppsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func NewAppsHeap() *AppsHeap {
	appsHeap := make(AppsHeap, 0)
	return &appsHeap
}
