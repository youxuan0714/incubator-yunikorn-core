package fair

import (
	"container/heap"
	"github.com/apache/yunikorn-core/pkg/custom/fair/urm/apps"
	"time"
	"strconv"
	"fmt"
)

func (f *FairManager) NextAppToScheduleByHRRN() (bool, string, string) {
	/*if f.unscheduledAppsIsEmpty(){
		return false, "", ""
	}
	success, user, target := f.FindMaxRRApp()
	if !success{
		return false,"",""
	}
	h := f.unscheduledApps[user]
	if _, exist := f.scheduledApps[target.ApplicationID]; exist {
		delete(f.scheduledApps, target.ApplicationID)
		if !f.unscheduledAppsIsEmpty() {
			success, user, target = f.FindMaxRRApp()
			if !success{
				return false,"",""
			}
			heap.Push(h, target)
		} else {
			return false, "", ""
		}
	} else {
		heap.Push(h, target)
	}*/

	user := f.getHighestUser()
	if user == ""{
		return false, "", ""
	}
	fmt.Println("user: ", user)
	h, ok := f.unscheduledApps[user]
	if !ok {
		f.unscheduledApps[user] = apps.NewAppsHeap()
		return false, "", ""
	}

	if h.Len() == 0 {
		return false, "", ""
	}

	target := heap.Pop(h).(*apps.AppInfo)
	if _, exist := f.scheduledApps[target.ApplicationID]; exist {
		delete(f.scheduledApps, target.ApplicationID)
		if h.Len() > 0 {
			target = heap.Pop(h).(*apps.AppInfo)
			heap.Push(h, target)
		} else {
			return false, "", ""
		}
	} else {
		heap.Push(h, target)
	}

	return true, user, target.ApplicationID
}

func (f *FairManager) unscheduledAppsIsEmpty() bool{
	f.Lock()
	f.Unlock()
	if len(f.unscheduledApps) == 0{
		return true
	}
	for _, h := range f.unscheduledApps{
		if h.Len() > 0{
			return false
		}
	}
	return true
}

func (f *FairManager) FindMaxRRApp() (bool, string, *apps.AppInfo){
	f.Lock()
	defer f.Unlock()
	var target *apps.AppInfo
	var targetuser string
	maxrr := 0.0
	now := time.Now()
	for user, _ := range f.unscheduledApps{
		h := f.unscheduledApps[user]
		if h.Len() == 0 {
			return false, "", nil
		}
		bk := make([]*apps.AppInfo, 0)
		for h.Len() > 0{
			dick := heap.Pop(h).(*apps.AppInfo)
			duration, err:= strconv.ParseFloat(dick.Duration, 64)
			if err != nil{
				return false, "", nil
			}
			rrtemp := CalautedResponseRatio(now, dick.SubmissionTime, duration)
			if rrtemp > maxrr {
				target = dick
				maxrr = rrtemp
				targetuser = user
			}
			bk = append(bk,dick)
		}
		for _, element := range bk {
			if element.ApplicationID != target.ApplicationID{
				heap.Push(h,element)
			}
		}
	}
	return true, targetuser, target
}

func (f *FairManager) NextAppToSchedule() (bool, string, string) {
	user := f.GetTenants().GetMinResourceUser(f.unscheduledApps, f.clusterResource)
	h, ok := f.unscheduledApps[user]
	if !ok {
		f.unscheduledApps[user] = apps.NewAppsHeap()
		return false, "", ""
	}

	if h.Len() == 0 {
		return false, "", ""
	}

	target := heap.Pop(h).(*apps.AppInfo)
	if _, exist := f.scheduledApps[target.ApplicationID]; exist {
		delete(f.scheduledApps, target.ApplicationID)
		if h.Len() > 0 {
			target = heap.Pop(h).(*apps.AppInfo)
			heap.Push(h, target)
		} else {
			return false, "", ""
		}
	} else {
		heap.Push(h, target)
	}

	return true, user, target.ApplicationID
}


func CalautedResponseRatio(now time.Time, subTime time.Time, duration float64) float64{
	waitingTime := now.Sub(subTime).Seconds()
	ResponseRatio := (waitingTime + duration) / duration
	return ResponseRatio
}

func (f *FairManager) getHighestUser() string {
	now := time.Now()
	highestRR := 0.0
	targetUser := ""
	userlist := [4]string{"user1", "user2", "user3", "user4"}
	for _, user := range userlist{
		h, ok:= f.unscheduledApps[user]
		if !ok{
			return ""
		}
		if h.Len() == 0{
			continue
		}
		dick := heap.Pop(h).(*apps.AppInfo)
		duration, err:= strconv.ParseFloat(dick.Duration, 64)
		if err != nil{
			return ""
		}
		tempRR := CalautedResponseRatio(now, dick.SubmissionTime, duration)
		if tempRR > highestRR{
			highestRR = tempRR
			targetUser = user
		}
	}
	return targetUser
}
