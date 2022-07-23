package edrf

import (
	"testing"
)

func TestDRF(t *testing.T) {
	var tasks = []Task{
		NewTask(
			"A",
			Resources{
				ResourceCPU:    1,
				ResourceMemory: 4,
			},
			nil,
			nil,
		),
		NewTask(
			"B",
			Resources{
				ResourceCPU:    3,
				ResourceMemory: 1,
			},
			nil,
			nil,
		),
	}
	cluster := NewCluster(
		Resources{
			ResourceCPU:    9,
			ResourceMemory: 18,
		},
		false,
	)

	e := New(cluster, tasks...)
	assignment := map[string]int{}
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		assignment[task.Name()]++
	}

	if cnt := assignment["A"]; cnt != 3 {
		t.Errorf("TaskA: want 3 but got %d", cnt)
	}
	if cnt := assignment["B"]; cnt != 2 {
		t.Errorf("TaskB: want 2 but got %d", cnt)
	}
}

func TestEDRFWithLimit(t *testing.T) {
	var tasks = []Task{
		NewTask(
			"A",
			Resources{
				ResourceCPU:    1,
				ResourceMemory: 4,
			},
			Resources{
				ResourceCPU: 2,
			},
			nil,
		),
		NewTask(
			"B",
			Resources{
				ResourceCPU:    3,
				ResourceMemory: 1,
			},
			nil,
			nil,
		),
	}
	cluster := NewCluster(
		Resources{
			ResourceCPU:    9,
			ResourceMemory: 18,
		},
		false,
	)
	e := New(cluster, tasks...)
	assignment := map[string]int{}
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		assignment[task.Name()]++
	}

	if cnt := assignment["A"]; cnt != 2 {
		t.Errorf("TaskA: want 3 but got %d", cnt)
	}
	if cnt := assignment["B"]; cnt != 2 {
		t.Errorf("TaskB: want 2 but got %d", cnt)
	}
}

func TestEDRFWithWeight(t *testing.T) {
	var tasks = []Task{
		NewTask(
			"A",
			Resources{
				ResourceCPU:    1,
				ResourceMemory: 2,
			},
			nil,
			map[ResourceType]float64{
				ResourceCPU: 3.0,
			},
		),
		NewTask(
			"B",
			Resources{
				ResourceCPU:    2,
				ResourceMemory: 1,
			},
			nil,
			map[ResourceType]float64{
				ResourceCPU: 1.0,
			},
		),
	}
	cluster := NewCluster(
		Resources{
			ResourceCPU:    9,
			ResourceMemory: 18,
		},
		false,
	)
	e := New(cluster, tasks...)
	assignment := map[string]int{}
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
		assignment[task.Name()]++
	}

	if cnt := assignment["A"]; cnt != 5 {
		t.Errorf("TaskA: want 5 but got %d", cnt)
	}
	if cnt := assignment["B"]; cnt != 2 {
		t.Errorf("TaskB: want 2 but got %d", cnt)
	}
	t.Log(e.Describe())
}
