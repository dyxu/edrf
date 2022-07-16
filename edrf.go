package edrf

import (
	"container/heap"
	"errors"
	"sync"
)

type EDRF interface {
	Assign() (Task, error)
	AddTask(t Task) error
	RemoveTask(t Task) error
	Describe() string
}

type Cluster interface {
	Capacity() Resources
}

type Task interface {
	Name() string
	Piece() Resources
}

type BinpackOption interface {
	Binpack() bool
}

type LimitOption interface {
	Limit() Resources
}

type WeightOption interface {
	Weight(ResourceType) float64
}

type ResourceType string
type ResourceAmount int64
type Resources map[ResourceType]ResourceAmount

const (
	ResourceCPU     ResourceType = "cpu"
	ResourceMemory  ResourceType = "memory"
	ResourceTraffic ResourceType = "traffic"
)

var (
	ErrEmptyTaskQueue = errors.New("empty task queue")
	ErrExistTask      = errors.New("exist task")
)

type taskWrap struct {
	Task
	allocated Resources
	limit     LimitOption
	weight    WeightOption
	dshare    float64
	index     int
	mu        sync.Mutex
}

func newTaskWrap(task Task) *taskWrap {
	w := &taskWrap{
		Task:      task,
		allocated: Resources{},
		dshare:    0.0,
		index:     -1,
	}
	if limit, ok := task.(LimitOption); ok {
		w.limit = limit
	}
	if weight, ok := task.(WeightOption); ok {
		w.weight = weight
	}

	return w
}

type taskQueue []*taskWrap

type eDRF struct {
	cluster   Cluster
	tasks     map[string]*taskWrap
	queue     taskQueue
	allocated Resources
	binpack   BinpackOption
	mu        sync.Mutex
}

func New(cluster Cluster, tasks ...Task) EDRF {
	e := &eDRF{
		cluster:   cluster,
		tasks:     map[string]*taskWrap{},
		queue:     []*taskWrap{},
		allocated: Resources{},
		mu:        sync.Mutex{},
	}

	if binpack, ok := cluster.(BinpackOption); ok {
		e.binpack = binpack
	}

	for _, t := range tasks {
		e.queue.Push(newTaskWrap(t))
	}
	heap.Init(&e.queue)

	return e
}

func (e *eDRF) Assign() (Task, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.queue) == 0 {
		return nil, ErrEmptyTaskQueue
	}

	t := e.queue[0]
	ok := e.tryAssignTo(t)
	if ok {
		e.computeDominantShare(t)
		heap.Fix(&e.queue, t.index)
	}

	return t, nil
}

func (e *eDRF) tryAssignTo(t *taskWrap) bool {
	piece := t.Piece()
	for k := range piece {
		if e.allocated[k]+piece[k] >= e.cluster.Capacity()[k] {
			return false
		}
	}

	for k := range piece {
		e.allocated[k] += piece[k]
		t.allocated[k] += piece[k]
	}

	return true
}

func (e *eDRF) computeDominantShare(t *taskWrap) {
	dshare := t.dshare
	for resource, allocated := range t.allocated {
		if amount, ok := e.allocated[resource]; ok && amount > 0 {
			dsharek := float64(allocated) / float64(amount)
			if t.weight != nil {
				dsharek = dsharek / t.weight.Weight(resource)
			}
			if dsharek > dshare {
				dshare = dsharek
			}
		}
	}
	t.dshare = dshare
}

func (e *eDRF) Describe() string {
	return ""
}

func (e *eDRF) AddTask(t Task) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.tasks[t.Name()]
	if ok {
		return ErrExistTask
	}

	e.queue.Push(newTaskWrap(t))
	return nil
}

func (tq taskQueue) Len() int { return len(tq) }

func (tq taskQueue) Less(i, j int) bool {
	return tq[i].dshare < tq[j].dshare
}

func (tq taskQueue) Swap(i, j int) {
	tq[i], tq[j] = tq[j], tq[i]
	tq[i].index = i
	tq[j].index = j
}

func (tq *taskQueue) Push(x interface{}) {
	n := len(*tq)
	item := x.(*taskWrap)
	item.index = n
	*tq = append(*tq, item)
}

func (tq *taskQueue) Pop() interface{} {
	old := *tq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*tq = old[0 : n-1]
	return item
}

func (tq *taskQueue) update(t *taskWrap, dshare float64) {
	t.dshare = dshare
	heap.Fix(tq, t.index)
}
