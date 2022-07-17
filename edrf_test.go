package edrf

import (
	"testing"
)

type fakeCluster struct {
	capacity Resources
}

func (c fakeCluster) Capacity() Resources {
	return c.capacity
}

func (c fakeCluster) Binpack() bool {
	return true
}

type fakeTask struct {
	name   string
	piece  Resources
	limit  Resources
	weight map[ResourceType]float64
}

func (t fakeTask) Name() string {
	return t.name
}

func (t fakeTask) Piece() Resources {
	return t.piece
}

func (t fakeTask) Limit(resourceType ResourceType) (ResourceAmount, bool) {
	if t.limit == nil {
		return 0, false
	}
	limit, ok := t.limit[resourceType]
	return limit, ok
}

func (t fakeTask) Weight(resourceType ResourceType) (float64, bool) {
	if t.weight == nil {
		return 0.0, false
	}
	weight, ok := t.weight[resourceType]
	return weight, ok
}

func TestDRF(t *testing.T) {
	var tasks = []Task{
		fakeTask{
			name: "TaskA",
			piece: Resources{
				ResourceCPU:    1,
				ResourceMemory: 4,
			},
		},
		fakeTask{
			name: "TaskB",
			piece: Resources{
				ResourceCPU:    3,
				ResourceMemory: 1,
			},
		},
	}
	cluster := fakeCluster{
		Resources{
			ResourceCPU:    9,
			ResourceMemory: 18,
		},
	}
	e := New(cluster, tasks...)
	assign := map[string]int{}
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		assign[task.Name()]++
	}

	if cnt := assign["TaskA"]; cnt != 3 {
		t.Errorf("TaskA: want 3 but got %d", cnt)
	}
	if cnt := assign["TaskB"]; cnt != 2 {
		t.Errorf("TaskB: want 2 but got %d", cnt)
	}
}

func TestEDRFWithLimit(t *testing.T) {
	var tasks = []Task{
		fakeTask{
			name: "TaskA",
			piece: Resources{
				ResourceCPU:    1,
				ResourceMemory: 4,
			},
			limit: Resources{
				ResourceCPU: 2,
			},
		},
		fakeTask{
			name: "TaskB",
			piece: Resources{
				ResourceCPU:    3,
				ResourceMemory: 1,
			},
		},
	}
	cluster := fakeCluster{
		Resources{
			ResourceCPU:    9,
			ResourceMemory: 18,
		},
	}
	e := New(cluster, tasks...)
	assign := map[string]int{}
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		assign[task.Name()]++
	}

	if cnt := assign["TaskA"]; cnt != 2 {
		t.Errorf("TaskA: want 3 but got %d", cnt)
	}
	if cnt := assign["TaskB"]; cnt != 2 {
		t.Errorf("TaskB: want 2 but got %d", cnt)
	}
}

func TestEDRFWithWeight(t *testing.T) {
	var tasks = []Task{
		fakeTask{
			name: "TaskA",
			piece: Resources{
				ResourceCPU:    1,
				ResourceMemory: 2,
			},
			weight: map[ResourceType]float64{
				ResourceCPU: 3.0,
			},
		},
		fakeTask{
			name: "TaskB",
			piece: Resources{
				ResourceCPU:    2,
				ResourceMemory: 1,
			},
			weight: map[ResourceType]float64{
				ResourceCPU: 1.0,
			},
		},
	}
	cluster := fakeCluster{
		Resources{
			ResourceCPU:    9,
			ResourceMemory: 18,
		},
	}
	e := New(cluster, tasks...)
	assign := map[string]int{}
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		assign[task.Name()]++
	}

	if cnt := assign["TaskA"]; cnt != 5 {
		t.Errorf("TaskA: want 5 but got %d", cnt)
	}
	if cnt := assign["TaskB"]; cnt != 2 {
		t.Errorf("TaskB: want 2 but got %d", cnt)
	}
	t.Log(e.Describe())
}
