package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	nReduce int
	mapTasks map[int]*Task // key: taskID
	reduceTasks map[int]*Task // key: taskID
	stage string
	mutex sync.Mutex
	timeout time.Duration
	wg sync.WaitGroup
}

// AssignTask RPC handler for the worker to call.
// Assigns task to worker
func (c *Coordinator) AssignTask(args *Task, reply *Task) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.stage == MAP {
		for _, task := range c.mapTasks {
			if task.Status == NOT_STARTED {
				task.TimeAssigned = time.Now()
				task.Status = IN_PROGRESS
				reply.TaskID = task.TaskID
				reply.Filepath = task.Filepath
				reply.TimeAssigned = task.TimeAssigned
				reply.Status = task.Status
				reply.Type = task.Type
				reply.NReduce = task.NReduce
				log.Printf("ASSIGNING TO WORKER: TASK #%d - Filepath: %v - Status: %v - Type: %v\n", reply.TaskID, reply.Filepath, reply.Status, reply.Type)
				return nil
			}
		}
	} else if c.stage == REDUCE {
		for _, task := range c.reduceTasks {
			if task.Status == NOT_STARTED {
				task.TimeAssigned = time.Now()
				task.Status = IN_PROGRESS
				reply.TaskID = task.TaskID
				reply.Filepath = task.Filepath
				reply.TimeAssigned = task.TimeAssigned
				reply.Status = task.Status
				reply.Type = task.Type
				reply.NReduce = task.NReduce
				log.Printf("ASSIGNING TO WORKER: TASK #%d - Filepath: %v - Status: %v - Type: %v\n", reply.TaskID, reply.Filepath, reply.Status, reply.Type)
				return nil
			}
		}

	} else if c.stage == COMPLETE {
		reply.Status = COMPLETE
	}
	return nil
}

// UpdateTaskStatus RPC handler for the worker to call.
// Updates the status of tasks once completed by a worker
func (c *Coordinator) UpdateTaskStatus(args *Task, reply *Task) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Status == COMPLETE {
		if args.Type == MAP {
			c.mapTasks[args.TaskID].Status = COMPLETE
			allComplete := true
			for _, task := range c.mapTasks {
				if task.Status != COMPLETE {
					allComplete = false
				}
			}
			if allComplete {
				log.Println("MAP TASKS ALL COMPLETE... SWITCHING TO REDUCE STAGE")
				c.stage = REDUCE
			}
		} else if args.Type == REDUCE {
			c.reduceTasks[args.TaskID].Status = COMPLETE
			allComplete := true
			for _, task := range c.reduceTasks {
				if task.Status != COMPLETE {
					allComplete = false
				}
			}
			if allComplete {
				log.Println("REDUCE TASKS ALL COMPLETE... SWITCHING TO COMPLETE STAGE")
				c.stage = COMPLETE
			}
		}
	}
	return nil
}

// checks if task progress has idled longer than timeout
func (c *Coordinator) checkTaskProgress() {
	defer c.mutex.Unlock()
	for {
		c.mutex.Lock()
		if c.stage == COMPLETE {
			break
		}
		for _, task := range c.mapTasks {
			if task.Status == IN_PROGRESS && task.TimeAssigned.Add(c.timeout).After(time.Now()) {
				task.Status = NOT_STARTED
			}
		}
		for _, task := range c.reduceTasks {
			if task.Status == IN_PROGRESS && task.TimeAssigned.Add(c.timeout).After(time.Now()) {
				task.Status = NOT_STARTED
			}
		}
		c.mutex.Unlock()
	}
	c.wg.Done()
}

//
// 'server' function provided in starter code:
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.stage == COMPLETE {
		log.Println("Tasks complete... quitting.")
		return true
	}
	return false
}

//
// create a coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.stage = MAP
	c.mapTasks = make(map[int]*Task)
	c.reduceTasks = make(map[int]*Task)
	c.timeout = 10 * time.Second

	log.Println("Generating map tasks...")
	for id, file := range files {
		mapTask := Task{}
		mapTask.Type = MAP
		mapTask.Filepath = file
		mapTask.TaskID = id
		mapTask.Status = NOT_STARTED
		mapTask.NReduce = c.nReduce
		c.mapTasks[id] = &mapTask
		log.Printf("TASK #%d - Type: %v - Status: %v - Filepath: %v\n", mapTask.TaskID, mapTask.Type, mapTask.Status, mapTask.Filepath)
	}

	log.Println("Generating reduce tasks...")
	for id:= 0; id < nReduce; id++ {
		reduceTask := Task{}
		reduceTask.Type = REDUCE
		reduceTask.TaskID = id
		reduceTask.Status = NOT_STARTED
		reduceTask.Filepath = "N/A"
		c.reduceTasks[id] = &reduceTask
		log.Printf("TASK #%d - Type: %v - Status: %v - Filepath: %v\n", reduceTask.TaskID, reduceTask.Type, reduceTask.Status, reduceTask.Filepath)
	}

	c.server()

	// runs a thread to check for tasks' progress in case of idle or crashed workers
	c.wg.Add(1)
	go c.checkTaskProgress()
	return &c
}
