package mem

import (
	"bytes"
	"fmt"
	"sync"
)

// TODO move these consts as config parameters to create pool
const (
	blockSizeInc = 8
	clusterCount = 32
)

// PoolConfig .
type poolConfig struct {
	// max num of blocks per group in cluster
	BlocksPerGroup uint16
	BlocksPerAlloc uint16
}

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
	sync.RWMutex
	pool        *Pool
	size        uint16
	groups      uint16
	muts        []*sync.Mutex
	blocks      [][]*Block
	totalBlocks uint32
	popIndex    uint32
	pushIndex   uint16
}

func newCluster(pool *Pool, size uint16) *cluster {
	groups := uint16(1)
	c := &cluster{
		pool:   pool,
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
	c.popIndex++
	poi := c.popIndex % uint32(c.groups)
	c.Unlock()

	c.muts[poi].Lock()
	index := len(c.blocks[poi]) - 1
	if index >= 0 {
		b, c.blocks[poi] = c.blocks[poi][index], c.blocks[poi][:index]
		c.muts[poi].Unlock()
	} else {
		c.muts[poi].Unlock()
		b = c.preAlloc()
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
	c.blocks[pui] = append(c.blocks[pui], b)
}

// pre-allocate and put blocks[1,blocksPerAlloc-1] to pool
func (c *cluster) preAlloc() (b *Block) {
	n := c.pool.Config.BlocksPerAlloc
	buf := make([]byte, n*c.size)
	c.pushPreAlloc(buf)
	// only return the first block (index 0)
	b = newBlock(c, buf[:c.size])

	c.Lock()
	defer c.Unlock()
	c.totalBlocks += uint32(n)
	if uint16(c.totalBlocks/uint32(c.pool.Config.BlocksPerGroup))+1 > c.groups {
		c.blocks = append(c.blocks, []*Block{})
		c.groups = uint16(len(c.blocks))
		c.muts = append(c.muts, new(sync.Mutex))
	}
	return
}

func (c *cluster) pushPreAlloc(buf []byte) {
	c.RLock()
	gi := 0
	min := len(c.blocks[0])
	for i := 1; i < len(c.blocks); i++ {
		l := len(c.blocks[i])
		if l < min {
			gi = i
			min = l
		}
	}
	c.RUnlock()

	c.muts[gi].Lock()
	defer c.muts[gi].Unlock()
	for i := uint16(1); i < c.pool.Config.BlocksPerAlloc; i++ {
		begin := int(i * c.size)
		bl := newBlock(c, buf[begin:begin+int(c.size)])
		c.blocks[gi] = append(c.blocks[gi], bl)
	}
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
	c.RLock()
	pop := c.popIndex
	alloc := c.totalBlocks
	c.RUnlock()
	b.WriteString(fmt.Sprintf("cluster{%d,groups:%d,pop:%d,len:%d/%d %v}",
		c.size, c.groups, pop, total, alloc, lens))
	return b.String()
}

// Pool .
type Pool struct {
	id       string
	clusters [clusterCount]*cluster
	Config   poolConfig
}

// NewPool .
func NewPool(id string) *Pool {
	p := &Pool{
		id: id,
		Config: poolConfig{
			BlocksPerGroup: 4096,
			BlocksPerAlloc: 32,
		},
	}
	for i := uint16(0); i < clusterCount; i++ {
		p.clusters[i] = newCluster(p, (i+1)*blockSizeInc)
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
