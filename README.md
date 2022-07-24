# eDRF
eDRF is an extended Dominant Resource Fairness（eDRF） implement in golang, allowing you to assignment with binpack, weight and limit options.

## Install

``` bash
go get github.com/dyxu/edrf
```

## Examples

### Interface

``` go
// EDRF defines the extended Dominant Resource Fairness interface
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

// Cluster defines a cluster
type Cluster interface {
	Capacity() Resources
}

// Task defines a task
type Task interface {
	Name() string
	Piece() Resources
}

// BinpackOption defines whether enbale binpack policy or not
type BinpackOption interface {
	Binpack() bool
}

// LimitOption returns the limit resouce of task
type LimitOption interface {
	Limit(ResourceType) (ResourceAmount, bool)
}

// WeightOption returns the weight of task
type WeightOption interface {
	Weight(ResourceType) (float64, bool)
}
```

### Standard DRF

``` go
import (
    "github.com/dyxu/edrf"
)

func main() {
	var tasks = []edrf.Task{
		edrf.NewTask(
			"A", // task name
			edrf.Resources{
				edrf.ResourceCPU:    1,
				edrf.ResourceMemory: 4,
			}, // piece
			nil, // limit
			nil, // weight
		),
		edrf.NewTask(
			"B",
			edrf.Resources{
				edrf.ResourceCPU:    3,
				edrf.ResourceMemory: 1,
			},
			nil,
			nil,
		),
	}
	cluster := edrf.NewCluster(
		edrf.Resources{
			edrf.ResourceCPU:    9,
			edrf.ResourceMemory: 18,
		}, // capacity
		false, // binpack policy
	)

	e := edrf.New(cluster, tasks...)
    allocated := map[string]edrf.Resources{}
    loop := 1
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
        fmt.Printf("Loop %.2d: assign to %s", loop, task.Name())
		alloc := allocated[task.Name()]
		alloc.Add(task.Piece())
		allocated[task.Name()] = alloc
        loop++
	}

    fmt.Print(e.Describe())
    fmt.Print(allocated)
}
```

### Extended DRF


``` go
import (
    "github.com/dyxu/edrf"
)

func main() {
	var tasks = []edrf.Task{
		edrf.NewTask(
			"A", // task name
			edrf.Resources{
				edrf.ResourceCPU:    1,
				edrf.ResourceMemory: 4,
			}, // piece
			edrf.Resources{
				edrf.ResourceCPU: 3,
			}, // limit
			map[edrf.ResourceType]float64{
				edrf.ResourceCPU: 3.0,
			}, // weight
		),
		edrf.NewTask(
			"B",
			edrf.Resources{
				edrf.ResourceCPU:    3,
				edrf.ResourceMemory: 1,
			},
            nil,
			map[edrf.ResourceType]float64{
				edrf.ResourceCPU: 1.0,
			},
		),
	}
	cluster := edrf.NewCluster(
		edrf.Resources{
			edrf.ResourceCPU:    18,
			edrf.ResourceMemory: 36,
		}, // capacity
		true, // binpack policy
	)

	e := edrf.New(cluster, tasks...)
    loop := 1
    allocated := map[string]edrf.Resources{}
	for {
		task, err := e.Assign()
		if err != nil {
			break
		}
        fmt.Printf("Loop %.2d: assign to %s", loop, task.Name())
		alloc := allocated[task.Name()]
		alloc.Add(task.Piece())
		allocated[task.Name()] = alloc
        loop++
	}

    fmt.Print(e.Describe())
    fmt.Print(allocated)
}
```