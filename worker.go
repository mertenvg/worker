package worker

import (
	"sort"
	"sync"
	"time"
)

type Option func(*Conf)

type Conf struct {
	QueueSize     uint
	Log           func(string, ...interface{})
	MaxWorkers    uint
	MinWorkers    uint
	AutoScale     bool
	IdleTimeout   time.Duration
	Step          uint
	CheckInterval time.Duration
}

var (
	NoopLogf = func(format string, args ...interface{}) {}
	defaults = Conf{
		QueueSize:     0,
		Log:           NoopLogf,
		MaxWorkers:    1,
		MinWorkers:    0,
		AutoScale:     false,
		IdleTimeout:   time.Duration(0),
		Step:          1,
		CheckInterval: time.Second,
	}
)

type HandleFunc func(data interface{})

type worker struct {
	active     bool
	dead       bool
	pool       *Pool
	kill       chan int
	lastActive time.Time
}

func (w *worker) work() {
	p := w.pool
	p.waitgroup.Add(1)
	defer func() {
		w.dead = true
		p.waitgroup.Done()
	}()

	for {
		select {
		case <-w.kill:
			return
		case data := <-p.queue:
			w.active = true

			p.workerFunc(data)

			w.active = false
			w.lastActive = time.Now()
		}
	}
}

// Pool represents a pool of workers
type Pool struct {
	workerFunc  HandleFunc
	conf        Conf
	workers     []*worker
	queue       chan interface{}
	waitgroup   sync.WaitGroup
	closeTicker chan int
	closing     bool
}

func (p *Pool) watch() {
	p.conf.Log("Starting watcher")
	t := time.NewTicker(p.conf.CheckInterval)
	for {
		select {
		case <-p.closeTicker:
			t.Stop()
			return
		case <-t.C:
			if p.conf.AutoScale {
				p.autoScale()
			}
		}
	}
}

// NewPool creates a new optionally auto scaling pool of workers
func NewPool(workerFunc HandleFunc, options ...Option) *Pool {
	c := defaults
	for _, o := range options {
		o(&c)
	}
	c.Log("Creating new pool with options: %+v", c)
	p := &Pool{
		workerFunc:  workerFunc,
		conf:        c,
		workers:     []*worker{},
		queue:       make(chan interface{}, c.QueueSize),
		closeTicker: make(chan int),
	}
	go p.watch()
	return p
}

// Queue adds work to the queue for the workers to pick up
func (p *Pool) Queue(work ...interface{}) {
	p.conf.Log("Adding %v item(s) to the queue", len(work))
	for _, w := range work {
		p.queue <- w
	}
}

// Grow pool by given number of worker slots
func (p *Pool) Grow(by uint) {
	p.conf.Log("Growing pool by %v workers", by)
	for i := uint(0); i < by; i++ {
		if uint(p.TotalCount()) >= p.conf.MaxWorkers {
			p.conf.Log("Reached the limit of %v workers. Unable to grow any further.", p.conf.MaxWorkers)
			return
		}
		worker := &worker{
			pool:       p,
			kill:       make(chan int),
			lastActive: time.Now(),
		}
		go worker.work()
		p.workers = append(p.workers, worker)
	}
}

// Shrink pool by given number of worker slots
func (p *Pool) Shrink(by uint) {
	p.conf.Log("Shrinking pool by %v workers", by)
	sort.Sort(byLastActiveDesc(p.workers))
	for i := uint(0); i < by; i++ {
		if p.TotalCount() == 0 {
			p.conf.Log("No workers left in the pool. Unable to shrink any further.")
			return
		}
		for _, w := range p.workers {
			if !w.dead {
				w.kill <- 1
				break
			}
		}
	}
}

// ActiveCount of active workers
func (p Pool) ActiveCount() int {
	count := 0
	for _, w := range p.workers {
		if w.active {
			count++
		}
	}
	return count
}

// IdleCount of idle workers
func (p Pool) IdleCount() int {
	return p.TotalCount() - int(p.ActiveCount())
}

// TotalCount of active and idle, but not dead workers
func (p Pool) TotalCount() int {
	count := 0
	for _, w := range p.workers {
		if !w.dead {
			count++
		}
	}
	return count
}

// QueueWaitingCount returns the number of work units waiting for a worker
func (p Pool) QueueWaitingCount() int {
	return len(p.queue)
}

// Clean removes dead workers and looks for idle workers to kill
func (p *Pool) Clean() {
	// cleanup dead workers
	workers := []*worker{}
	for _, w := range p.workers {
		if !w.dead {
			workers = append(workers, w)
		}
	}
	if len(p.workers)-len(workers) > 0 {
		p.conf.Log("Cleaned up %v dead workers", len(p.workers)-len(workers))
	}
	p.workers = workers

	// cleanup workers idle for too long
	if int(p.conf.IdleTimeout) > 0 {
		now := time.Now()
		count := uint(0)
		canKill := uint(p.TotalCount()) - p.conf.MinWorkers
		for _, w := range p.workers {
			if !w.active && now.Sub(w.lastActive) > p.conf.IdleTimeout && count < canKill {
				if w.dead {
					continue
				}
				w.kill <- 1
				count++
			}
		}
		if count > 0 {
			p.conf.Log("Killed %v idle workers", count)
		}
	}
}

func (p *Pool) autoScale() {
	if !p.conf.AutoScale {
		p.conf.Log("Auto scaling is not enabled for this pool.")
		return
	}
	p.Clean()
	// queue size above 90% of max queue size. Try growing the pool.
	if uint(len(p.queue)) > uint(float32(p.conf.QueueSize)*0.9) {
		p.conf.Log("Queue is filled above 90%%. Adding %v workers", p.conf.Step)
		p.Grow(p.conf.Step)
	}
	// number of workers is below the minimum required. Try growing the pool.
	if uint(p.TotalCount()) < p.conf.MinWorkers {
		p.conf.Log("Queue is below minimum number of workers threshhold. Adding %v workers", p.conf.MinWorkers-uint(p.TotalCount()))
		p.Grow(p.conf.MinWorkers - uint(p.TotalCount()))
	}
}

// Stop accepting new work, finish processing the queue and gracefully shutdown the pool
func (p *Pool) Stop() {
	p.conf.Log("Closing down the pool")
	p.closeTicker <- 1
	p.closing = true
	if p.QueueWaitingCount() > 0 {
		for i := 0; i < 30; i++ {
			p.conf.Log("Waiting %s for queue to empty...", time.Duration(int(time.Second)*(30-i)))
			if p.QueueWaitingCount() == 0 {
				break
			}
			time.Sleep(time.Second)
		}
	}
	p.conf.Log("Queue is empty. Killing all workers")
	for _, w := range p.workers {
		if w.dead {
			// cant kill it if it's already dead
			continue
		}
		w.kill <- 1
	}
	p.Wait()
}

// Wait for all workers to finish before exiting
func (p *Pool) Wait() {
	p.conf.Log("Waiting for workers to finish")
	p.waitgroup.Wait()
}

// Configure the pool
func (p *Pool) Configure(options ...Option) {
	for _, o := range options {
		o(&p.conf)
	}
}

// WithLogFunc overrides the default log.Printf function used for logging to your own preferred logging function
func WithLogFunc(f func(string, ...interface{})) Option {
	return func(c *Conf) {
		c.Log = f
	}
}

// WithLimits set the maximum allowed queue length and workers count. This may block your application when adding
// work to a queue that is already full
func WithLimits(queue, workersMin, workersMax uint) Option {
	return func(c *Conf) {
		c.QueueSize = queue
		c.MinWorkers = workersMin
		c.MaxWorkers = workersMax
	}
}

// WithAutoScale enables the auto scaling of the workers pool to accommodate the amount of traffic on the queue.
func WithAutoScale(timeout time.Duration, step uint) Option {
	return func(c *Conf) {
		c.AutoScale = true
		c.IdleTimeout = timeout
		c.Step = step
	}
}

// WithCheckInterval overrides the default check interval of 10ms to one of your choosing.
func WithCheckInterval(interval time.Duration) Option {
	return func(c *Conf) {
		c.CheckInterval = interval
	}
}

type byLastActiveDesc []*worker

func (a byLastActiveDesc) Len() int      { return len(a) }
func (a byLastActiveDesc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byLastActiveDesc) Less(i, j int) bool {
	return a[i].active || a[i].lastActive.After(a[j].lastActive)
}
