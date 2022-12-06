package apps

import (
	"container/heap"
	"testing"
	"time"
)

func TestNewAppInfo(t *testing.T) {
	type inputs struct {
		id             string
		submissionTime time.Time
	}
	tests := []struct {
		caseName string
		input    inputs
	}{
		{"normal", inputs{"application-01", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)}},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			tmp := NewAppInfo(test.input.id, test.input.submissionTime)
			if tmp.ApplicationID != test.input.id || !tmp.SubmissionTime.Equal(test.input.submissionTime) {
				t.Errorf("Expected %s %v, got %s %v",
					test.input.id,
					test.input.submissionTime,
					tmp.ApplicationID,
					tmp.SubmissionTime)
			}
		})
	}
}

func TestAppsHeap(t *testing.T) {
	type input struct {
		id             string
		submissionTime time.Time
	}
	tests := []struct {
		caseName string
		apps     []input
		expected []string
	}{
		{
			"normal",
			[]input{
				input{"application-01", time.Date(2020, time.November, 10, 23, 0, 0, 0, time.UTC)},
				input{"application-02", time.Date(2009, time.November, 11, 23, 0, 0, 0, time.UTC)},
				input{"application-03", time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC)},
			},
			[]string{"application-02", "application-03", "application-01"},
		},
		{
			"same time",
			[]input{
				input{"application-02", time.Date(2020, time.November, 10, 23, 0, 0, 0, time.UTC)},
				input{"application-01", time.Date(2020, time.November, 10, 23, 0, 0, 0, time.UTC)},
				input{"application-03", time.Date(2020, time.November, 10, 23, 0, 0, 0, time.UTC)},
			},
			[]string{"application-01", "application-02", "application-03"},
		},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			h := NewAppsHeap()
			for _, element := range test.apps {
				heap.Push(h, NewAppInfo(element.id, element.submissionTime))
			}

			if len(test.expected) != h.Len() {
				t.Errorf("expected len %d,got %d", len(test.expected), h.Len())
			} else {
				for i := 0; h.Len() > 0; i++ {
					tmp := heap.Pop(h).(*AppInfo)
					if tmp.ApplicationID != test.expected[i] {
						t.Errorf("Expected app: %s, got %s", tmp.ApplicationID, test.expected[i])
					}
				}
			}
		})
	}
}
