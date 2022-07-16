package edrf

import (
	"container/heap"
	"errors"
	"sync"
)

type EDRF interface {
	Solve() ([]Task, error)
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

func newTaskWrap(task Task, index int) *taskWrap {
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
	tasks     taskQueue
	allocated Resources
	binpack   BinpackOption
	mu        sync.Mutex
}

func New(cluster Cluster, tasks ...Task) EDRF {
	e := &eDRF{
		cluster:   cluster,
		tasks:     []*taskWrap{},
		allocated: Resources{},
		mu:        sync.Mutex{},
	}

	if binpack, ok := cluster.(BinpackOption); ok {
		e.binpack = binpack
	}

	for i, t := range tasks {
		e.tasks = append(e.tasks, newTaskWrap(t, i))
	}
	heap.Init(&e.tasks)

	return e
}

func (e *eDRF) Solve() ([]Task, error) {
	var tasks []Task
	for {
		task, err := e.solveOnce()
		if err != nil {
			break
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (e *eDRF) solveOnce() (Task, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.tasks) == 0 {
		return nil, ErrEmptyTaskQueue
	}

	t := e.tasks[0]
	ok := e.tryAllocateFor(t)
	if ok {
		e.computeDominantShare(t)
		heap.Fix(&e.tasks, t.index)
	}

	return t, nil
}

func (e *eDRF) tryAllocateFor(t *taskWrap) bool {
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
