package users

import (
	"testing"
)

func TestNewUserHeap(t *testing.T) {
	h := NewUserHeap()
	if h.Len() != 0 {
		t.Error("default len of user heap should be 0")
	}
}
