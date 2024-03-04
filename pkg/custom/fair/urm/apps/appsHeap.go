package apps

import (
	"time"
	"strconv"
)

type AppInfo struct {
	ApplicationID  string
	SubmissionTime time.Time
	Duration	   string
}

func NewAppInfo(id string, t time.Time, d string) *AppInfo {
	return &AppInfo{
		ApplicationID:  id,
		SubmissionTime: t,
		Duration:		d,
	}
}

type AppsHeap []*AppInfo

func (h AppsHeap) Len() int { return len(h) }
func (h AppsHeap) Less(i, j int) bool {
	now := time.Now()
	rri := CalautedResponseRatio(now, h[i].SubmissionTime, h[i].Duration)
	rrj := CalautedResponseRatio(now, h[j].SubmissionTime, h[j].Duration)
	if rri == rrj {
		return h[i].ApplicationID < h[j].ApplicationID
	}
	return rri > rrj
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

func CalautedResponseRatio(now time.Time, subTime time.Time, duration string) float64{
	d, err := strconv.ParseFloat(duration, 64)
	if err != nil{
		return 0.0
	}
	waitingTime := now.Sub(subTime).Seconds()
	ResponseRatio := (waitingTime + d) / d
	return ResponseRatio
}
