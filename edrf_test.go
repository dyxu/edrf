package edrf

import (
	"testing"
)

type cluster struct {
}

func (c cluster) Capacity() Resources {
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
		ResourceMemory: 4,
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
	var tasks = []Task{taskA{}, taskB{}}
	e := New(cluster{}, tasks...)
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		t.Logf("Assign to %s", task.Name())
	}
}
