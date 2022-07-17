package edrf

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"sync"
)

type EDRF interface {
	// Assign assigns an piece of resource to task
	Assign() (Task, error)
	// AddTask adds a task
	AddTask(t Task) error
	// RemoveTask removes a task
	RemoveTask(t Task) error
	// Describe describes the eDRF detail
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
	Limit(ResourceType) (ResourceAmount, bool)
}

type WeightOption interface {
	Weight(ResourceType) (float64, bool)
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
	ErrNoAssignableTask = errors.New("no assignable task")
	ErrExistTask        = errors.New("task exists")
	ErrTaskNotFound     = errors.New("task not found")
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
	limit     func(ResourceType) (ResourceAmount, bool)
	weight    func(ResourceType) (float64, bool)
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
		w.limit = limit.Limit
	}
	if weight, ok := task.(WeightOption); ok {
		w.weight = weight.Weight
	}

	return w
}

func (t *taskWrap) reachLimit() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.limit != nil {
		for k, v := range t.allocated {
			limit, ok := t.limit(k)
			if ok && t.piece[k]+v > limit {
				return true
			}
		}
	}
	return false
}

func (t *taskWrap) incr() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, v := range t.piece {
		t.allocated[k] += v
	}
}

type taskQueue []*taskWrap

type eDRF struct {
	tasks     map[string]*taskWrap
	queue     taskQueue
	capacity  Resources
	allocated Resources
	binpack   bool
	mu        sync.Mutex
}

func New(cluster Cluster, tasks ...Task) EDRF {
	e := &eDRF{
		tasks:     map[string]*taskWrap{},
		queue:     taskQueue{},
		capacity:  DeepCopyResourcesTo(cluster.Capacity()),
		allocated: Resources{},
		binpack:   false,
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
	for e.queue.Len() > 0 {
		t := heap.Pop(&e.queue).(*taskWrap)
		ok := e.tryAssignTo(t)
		if ok {
			e.computeDominantShare(t)
			if !t.reachLimit() {
				heap.Push(&e.queue, t)
			}

			return t.task, nil
		}

		// Enable binpack policy
		if !e.binpack {
			return nil, ErrNoAssignableTask
		}
	}

	return nil, ErrNoAssignableTask
}

func (e *eDRF) tryAssignTo(t *taskWrap) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k := range t.piece {
		if e.allocated[k]+t.piece[k] > e.capacity[k] {
			return false
		}
	}

	for k := range t.piece {
		e.allocated[k] += t.piece[k]
	}

	t.incr()

	return true
}

func (e *eDRF) computeDominantShare(t *taskWrap) {
	dshare := t.dshare
	for resource, allocated := range t.allocated {
		if amount, ok := e.capacity[resource]; ok && amount > 0 {
			dsharek := float64(allocated) / float64(amount)
			if t.weight != nil {
				if weight, ok := t.weight(resource); ok {
					dsharek = dsharek / weight
				}
			}
			if dsharek > dshare {
				dshare = dsharek
			}
		}
	}
	t.dshare = dshare
}

func (e *eDRF) Describe() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	sb := new(bytes.Buffer)

	fmt.Fprintf(sb, "Cluster: capacity=%v, allocated=%v, binpack=%v\n", e.capacity, e.allocated, e.binpack)
	for _, tw := range e.tasks {
		fmt.Fprintf(sb, "%s: piece=%v, allocated=%v\n", tw.task.Name(), tw.piece, tw.allocated)
	}

	return sb.String()
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
	heap.Push(&e.queue, tw)

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
