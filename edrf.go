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
	Limit(ResourceType) ResourceAmount
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
	ErrEmptyTaskQueue        = errors.New("empty task queue")
	ErrExistTask             = errors.New("task exists")
	ErrTaskNotFound          = errors.New("task not found")
	ErrExhaustionOfResources = errors.New("exhaustion of resources")
)

func DeepCopyResourcesTo(from Resources) Resources {
	to := Resources{}
	for k, v := range from {
		to[k] = v
	}

	return to
}

type taskWrap struct {
	task      Task
	piece     Resources
	allocated Resources
	limit     LimitOption
	weight    WeightOption
	dshare    float64
	index     int
	mu        sync.Mutex
}

func newTaskWrap(task Task) *taskWrap {
	w := &taskWrap{
		task:      task,
		piece:     DeepCopyResourcesTo(task.Piece()),
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

func (t *taskWrap) reachLimit() bool {
	if t.limit != nil {
		for k, v := range t.allocated {
			if t.piece[k]+v >= t.limit.Limit(k) {
				return true
			}
		}
	}
	return false
}

type taskQueue []*taskWrap

type eDRF struct {
	capacity  Resources
	tasks     map[string]*taskWrap
	queue     taskQueue
	allocated Resources
	binpack   bool
	mu        sync.Mutex
}

func New(cluster Cluster, tasks ...Task) EDRF {
	e := &eDRF{
		capacity:  DeepCopyResourcesTo(cluster.Capacity()),
		tasks:     map[string]*taskWrap{},
		queue:     taskQueue{},
		allocated: Resources{},
		mu:        sync.Mutex{},
	}

	if binpack, ok := cluster.(BinpackOption); ok {
		e.binpack = binpack.Binpack()
	}

	for _, t := range tasks {
		tw := newTaskWrap(t)
		e.tasks[t.Name()] = tw
		e.queue.Push(tw)
	}
	heap.Init(&e.queue)

	return e
}

func (e *eDRF) Assign() (Task, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.queue.Len() == 0 {
		return nil, ErrEmptyTaskQueue
	}

	t := e.queue.Back()
	ok := e.tryAssignTo(t)
	if ok {
		e.computeDominantShare(t)
		heap.Fix(&e.queue, t.index)
	} else if !e.binpack {
		return nil, ErrExhaustionOfResources
	}

	if t.reachLimit() {
		heap.Remove(&e.queue, t.index)
	}

	return t.task, nil
}

func (e *eDRF) tryAssignTo(t *taskWrap) bool {
	for k := range t.piece {
		if e.allocated[k]+t.piece[k] >= e.capacity[k] {
			return false
		}
	}

	for k := range t.piece {
		e.allocated[k] += t.piece[k]
		t.allocated[k] += t.piece[k]
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

	tw := newTaskWrap(t)
	e.tasks[t.Name()] = tw
	e.queue.Push(tw)

	return nil
}

func (e *eDRF) RemoveTask(t Task) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	tw, ok := e.tasks[t.Name()]
	if !ok {
		return ErrTaskNotFound
	}

	heap.Remove(&e.queue, tw.index)
	tw.index = -1
	for k, v := range tw.allocated {
		e.allocated[k] -= v
	}
	delete(e.tasks, t.Name())

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

func (tq taskQueue) Back() *taskWrap {
	return tq[tq.Len()-1]
}
