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
	tasksToRetry := make([]*DoTaskArgs, 0)
	failureCounts := WorkerFailureCount{workers: make(map[string]int)}

	// Helper to start task
	startTask := func(worker string, args *DoTaskArgs) {
		defer waitGroup.Done()
		success := call(worker, "Worker.DoTask", args, nil)
		readyChan <- worker
		if !success {
			failureCounts.inc(worker)
			retryChan <- args
		}
	}

	// Helper to get args for next task - returns (isRetry, args)
	getNextTaskArgs := func(worker string, currentTask int) (bool, *DoTaskArgs) {
		if len(tasksToRetry) > 0 {
			index := len(tasksToRetry) - 1
			args := tasksToRetry[index]
			tasksToRetry = tasksToRetry[:index]
			fmt.Printf("Schedule: %v task #%d attempting to be retried by %s\n", phase, args.TaskNumber, worker)
			return true, args
		}
		return false, &DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[currentTask], // Ignored for reduce phase
			Phase:         phase,
			TaskNumber:    currentTask,
			NumOtherPhase: nOther,
		}
	}

	// Assign tasks to registered + available workers
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)
	for currentTask := 0; currentTask < ntasks; {
		select {
		case taskArgs := <-retryChan: // Add task to retry list
			tasksToRetry = append(tasksToRetry, taskArgs)
		case worker := <-registerChan: // New worker registering, set them as available
			readyChan <- worker
		case worker := <-readyChan: // Worker is ready to start another task
			if failureCounts.get(worker) < MaxWorkerFailures {
				waitGroup.Add(1)
				isRetry, args := getNextTaskArgs(worker, currentTask)
				if !isRetry {
					currentTask++
				}
				go startTask(worker, args)
			}
		default:
		}
	}

	// Wait for all in-flight tasks to finish
	waitGroup.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
