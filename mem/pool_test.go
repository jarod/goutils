package mem

import (
	"testing"
	"time"
)

func TestClusterPop(t *testing.T) {
	p := NewPool("TestClusterPop")
	p.Config.BlocksPerAlloc = 2
	c := newCluster(p, 16)
	b1 := c.Pop()
	b2 := c.Pop()
	l := len(c.blocks[0])
	if l != 0 {
		t.Error("cluster should have 0 block, len=", l)
	}
	b1.Release()
	b2.Retain()
	l = len(c.blocks[0])
	if l != 1 {
		t.Error("cluster should have 1 block, len=", l)
	}
	b2.Release()
	b2.Release()
	b1 = c.Pop()
	l = len(c.blocks[0])
	if l != 1 {
		t.Error("cluster should have 1 block, len=", l)
	}
}

func TestClusterBlockGroupGrow(t *testing.T) {
	p := NewPool("TestClusterBlockGroupGrow")
	p.Config.BlocksPerGroup = 2
	p.Config.BlocksPerAlloc = 3
	c := newCluster(p, 16)
	c.Pop()
	time.Sleep(time.Millisecond * 10)
	if c.groups != 2 || len(c.blocks) != 2 || len(c.muts) != 2 {
		t.Error("block group not grow as expected, group=", c.groups)
	}
	c.Pop()
	l := len(c.blocks[1])
	if l != 0 {
		t.Error("blocks[1] should have len=0,actual", l)
	}
	c.Pop()
	time.Sleep(time.Millisecond * 10)
	l = len(c.blocks[1])
	if l != int(p.Config.BlocksPerAlloc-1) {
		t.Errorf("blocks[1] should have len=%d,actual %d", p.Config.BlocksPerAlloc-1, l)
	}
}
