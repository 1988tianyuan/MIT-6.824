package mapreduce

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const MrRpcName = "Worker.DoTask"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var nTasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		nTasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		nTasks = nReduce
		nOther = len(mapFiles)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, nOther)
	taskWg := sync.WaitGroup{}
	taskWg.Add(nTasks)
	taskChan := make(chan DoTaskArgs, nTasks)
	for i:=0; i<nTasks; i++ {
		args := makeArgs(jobName, mapFiles, phase, i, nOther, i)
		taskChan <- args
	}

	remainedTask := int64(nTasks)
	go fetchWorkers(registerChan, func(worker string) {
		for remainedTask > 0 {
			args := <- taskChan
			ok := call(worker, MrRpcName, args, nil)
			if ok == false {
				fmt.Printf("Schedule: call worker to doTask %v failed, task file is: {%v}, let other work do this work\n", phase, args.File)
				taskChan <- args
			} else {
				taskWg.Done()
				atomic.CompareAndSwapInt64(&remainedTask, remainedTask, remainedTask-1)
			}
		}
		fmt.Printf("no task for %s task\n", phase)
	})
	taskWg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

func fetchWorkers(registerChan chan string, sendTask func(worker string))  {
	for {
		worker := <- registerChan
		fmt.Println("one worker has been found: ", worker)
		go sendTask(worker)
	}
}

func makeArgs(jobName string, mapFiles []string, phase jobPhase, taskIndex int, nOther int, index int) DoTaskArgs {
	args := DoTaskArgs{jobName, "", phase, taskIndex, nOther}
	if phase == mapPhase {
		args.File = mapFiles[index]
	}
	return args
}
