package mapreduce

import (
	"fmt"
	"sync"
)

type WorkerFailureCount struct {
	sync.Mutex
	workers map[string]int // worker srv -> # failures
}

func (r *WorkerFailureCount) inc(srv string) {
	r.Lock()
	defer r.Unlock()
	r.workers[srv]++
}

func (r *WorkerFailureCount) get(srv string) int {
	r.Lock()
	defer r.Unlock()
	return r.workers[srv]
}

const MaxWorkerFailures = 5

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(
	jobName string,
	mapFiles []string,
	nReduce int,
	phase jobPhase,
	registerChan chan string,
) {
	var waitGroup sync.WaitGroup
	var ntasks, nOther int
	if phase == mapPhase {
		ntasks, nOther = len(mapFiles), nReduce
	} else {
		ntasks, nOther = nReduce, len(mapFiles)
	}

	// Ready worker channel. Buffer size = number of tasks
	readyChan := make(chan string, ntasks)
	retryChan := make(chan *DoTaskArgs, ntasks)
	failureCounts := WorkerFailureCount{workers: make(map[string]int)}

	// Helper to start task
	startTask := func(worker string, args *DoTaskArgs) {
		defer waitGroup.Done()
		success := call(worker, "Worker.DoTask", args, nil)
		readyChan <- worker
		if !success {
			fmt.Printf("Schedule: %v task #%d failed to execute by %s\n", phase, args.TaskNumber, worker)
			failureCounts.inc(worker)
			retryChan <- args
		}
	}

	// Create task list
	tasks := make([]*DoTaskArgs, 0)
	for currentTask := 0; currentTask < ntasks; currentTask++ {
		args := &DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[currentTask], // Ignored for reduce phase
			Phase:         phase,
			TaskNumber:    currentTask,
			NumOtherPhase: nOther,
		}
		tasks = append(tasks, args)
	}

	// Assign tasks to registered + available workers
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)
	for len(tasks) > 0 {
		select {
		case taskArgs := <-retryChan: // Add "retry" task back to task list
			tasks = append(tasks, taskArgs)
		case worker := <-registerChan: // New worker is registering, forward them to ready channel
			readyChan <- worker
		case worker := <-readyChan:
			if failureCounts.get(worker) < MaxWorkerFailures {
				// Pop from back of task and execute
				waitGroup.Add(1)
				index := len(tasks) - 1
				args := tasks[index]
				tasks = tasks[:index]
				go startTask(worker, args)
			} else {
				// Workers that have failed more than `MaxWorkerFailures` will not be added back to ready worker channel
				fmt.Printf("Schedule: %v, Worker %s failed %d times and will no longer be used\n", phase, worker, MaxWorkerFailures)
			}
		}
	}

	// Wait for all in-flight tasks to finish
	waitGroup.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
