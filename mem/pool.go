package mem

import (
	"bytes"
	"fmt"
	"sync"
)

// TODO move these consts as config parameters to create pool
const (
	blocksPerAlloc = 8
	blocksInGroup  = 4096
	blockSizeInc   = 16
	clusterCount   = 32
	clusterMaxSize = 1048576 // 1M
)

// Block .
type Block struct {
	sync.Mutex
	ref  int
	data []byte
	c    *cluster
	size uint16 // requested size
}

func newBlock(c *cluster, data []byte) *Block {
	b := &Block{
		c:    c,
		data: data,
	}
	return b
}

func newTempBlock(size uint16) *Block {
	b := &Block{
		data: make([]byte, size),
		size: size,
	}
	return b
}

func (b *Block) String() string {
	b.Lock()
	defer b.Unlock()
	return fmt.Sprintf("Block{Cap:%d,Size:%d,Ref:%d}", len(b.data), b.size, b.ref)
}

// Retain .
func (b *Block) Retain() {
	b.Lock()
	b.ref++
	b.Unlock()
}

// Release .
func (b *Block) Release() {
	b.Lock()
	defer b.Unlock()
	b.ref--
	if b.ref == 0 && b.c != nil {
		b.c.push(b)
	}
}

// Buffer .
func (b *Block) Buffer() []byte {
	b.Lock()
	defer b.Unlock()
	return b.data[:b.size]
}

type cluster struct {
	sync.Mutex
	size                uint16
	groups              uint16
	muts                []*sync.Mutex
	blocks              [][]*Block
	popIndex, pushIndex uint16
}

func newCluster(size uint16) *cluster {
	groups := uint16(clusterMaxSize/uint32(size)/blocksInGroup + 1)
	c := &cluster{
		size:   size,
		groups: groups,
		muts:   make([]*sync.Mutex, groups),
		blocks: make([][]*Block, groups),
	}
	for i := uint16(0); i < groups; i++ {
		c.muts[i] = new(sync.Mutex)
	}
	return c
}

func (c *cluster) Pop() (b *Block) {
	c.Lock()
	c.popIndex = (c.popIndex + 1) % c.groups
	poi := c.popIndex
	c.Unlock()

	c.muts[poi].Lock()
	index := len(c.blocks[poi]) - 1
	if index >= 0 {
		b, c.blocks[poi] = c.blocks[poi][index], c.blocks[poi][:index]
		c.muts[poi].Unlock()
	} else {
		c.muts[poi].Unlock()

		// pre-allocation and put blocks[1,blocksPerAlloc-1] to pool
		buf := make([]byte, c.size*blocksPerAlloc)
		for i := 1; i < blocksPerAlloc; i++ {
			begin := i * int(c.size)
			b = newBlock(c, buf[begin:begin+int(c.size)])
			c.push(b)
		}
		// only return the first block (index 0)
		b = newBlock(c, buf[:c.size])
	}
	b.Retain()
	return
}

func (c *cluster) push(b *Block) {
	c.Lock()
	c.pushIndex = (c.pushIndex + 1) % c.groups
	pui := c.pushIndex
	c.Unlock()

	c.muts[pui].Lock()
	defer c.muts[pui].Unlock()
	l := len(c.blocks[pui])
	if l >= blocksInGroup {
		return
	}
	c.blocks[pui] = append(c.blocks[pui], b)
}

func (c *cluster) String() string {
	var b bytes.Buffer
	lens := make([]uint16, c.groups)
	total := uint32(0)
	for i, m := range c.muts {
		m.Lock()
		defer m.Unlock()
		lens[i] = uint16(len(c.blocks[i]))
		total += uint32(lens[i])
	}
	b.WriteString(fmt.Sprintf("cluster{%d,groups:%d,len:%d %v}",
		c.size, c.groups, total, lens))
	return b.String()
}

// Pool .
type Pool struct {
	id       string
	clusters [clusterCount]*cluster
}

// NewPool .
func NewPool(id string) *Pool {
	p := &Pool{
		id: id,
	}
	for i := uint16(0); i < clusterCount; i++ {
		p.clusters[i] = newCluster((i + 1) * blockSizeInc)
	}
	return p
}

// Alloc .
func (p *Pool) Alloc(size uint16) *Block {
	index := size / blockSizeInc
	if index >= clusterCount {
		b := newTempBlock(size)
		b.Retain()
		return b
	}
	b := p.clusters[index].Pop()
	b.size = size
	return b
}

func (p *Pool) String() string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("Pool{id:%s,clusters:%d}\n", p.id, len(p.clusters)))
	for _, c := range p.clusters {
		b.WriteString(c.String())
		b.WriteRune('\n')
	}
	return b.String()
}
