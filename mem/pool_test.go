package mem

import (
	"testing"
)

func TestClusterPop(t *testing.T) {
	c := newCluster(16)
	b1 := c.Pop()
	b2 := c.Pop()
	l := len(c.blocks)
	if l != 0 {
		t.Error("cluster should have 0 block, len=", l)
	}
	b1.Release()
	b2.Retain()
	l = len(c.blocks)
	if l != 1 {
		t.Error("cluster should have 1 block, len=", l)
	}
	b2.Release()
	b2.Release()
	b1 = c.Pop()
	if l != 1 {
		t.Error("cluster should have 1 block, len=", l)
	}
}
