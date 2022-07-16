package edrf

import (
	"testing"
)

type fakeCluster struct {
}

func (c fakeCluster) Capacity() Resources {
	return Resources{
		ResourceCPU:    9,
		ResourceMemory: 18,
	}
}

type taskA struct {
}

func (taskA) Name() string {
	return "taskA"
}

func (taskA) Piece() Resources {
	return Resources{
		ResourceCPU:    1,
		ResourceMemory: 3,
	}
}

type taskB struct {
}

func (taskB) Name() string {
	return "taskB"
}

func (taskB) Piece() Resources {
	return Resources{
		ResourceCPU:    3,
		ResourceMemory: 1,
	}
}

func TestDRF(t *testing.T) {
	var tasks []Task = []Task{
		taskA{},
		taskB{},
	}
	e := New(fakeCluster{}, tasks...)
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		t.Logf("Assign to %s", task.Name())
	}
}
