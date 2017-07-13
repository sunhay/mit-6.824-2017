package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

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

	// Helper to start task
	startTask := func(worker string, args *DoTaskArgs) {
		defer waitGroup.Done()
		result := call(worker, "Worker.DoTask", args, nil)
		readyChan <- worker
		log.Printf("[%s, %s Scheduler] Task %d finished with result %t\n", jobName, phase, args.TaskNumber, result)
	}

	// Assign tasks to registered + available workers
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)
	for currentTask := 0; currentTask < ntasks; {
		select {
		case worker := <-registerChan: // New worker registering, set them as available
			readyChan <- worker
		case worker := <-readyChan: // Worker is ready to start another task
			args := DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[currentTask], // Ignored for reduce phase
				Phase:         phase,
				TaskNumber:    currentTask,
				NumOtherPhase: nOther,
			}
			waitGroup.Add(1)
			currentTask++
			go startTask(worker, &args)
		}
	}

	// Wait for all in-flight tasks to finish
	waitGroup.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
