package routine

// Pool ...
type Pool struct {
	s         uint
	jobChan   chan func()
	closeChan chan bool
}

// NewPool ...
func NewPool(nums uint, maxJob uint) *Pool {
	return &Pool{
		s:         nums,
		jobChan:   make(chan func(), maxJob),
		closeChan: make(chan bool),
	}
}

// Start ...
func (wp *Pool) Start() {
	for i := uint(0); i < wp.s; i++ {
		go wp.dispatch()
	}
}

func (wp *Pool) dispatch() {
FOR:
	for {
		select {
		case job := <-wp.jobChan:
			job()
		case <-wp.closeChan:
			break FOR
		}
	}
}

// Close ...
func (wp *Pool) Close() {
	close(wp.closeChan)
	close(wp.jobChan)
}

// Push ...
func (wp *Pool) Push(job func()) {
	wp.jobChan <- job
}

// Length ...
func (wp *Pool) Length() int {
	return len(wp.jobChan)
}
