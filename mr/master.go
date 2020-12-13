package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	nReduce int
	mapTasks map[int]*Task // key: taskID
	reduceTasks map[int]*Task // key: taskID
	stage string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *Task, reply *Task) error {

	if m.stage == MAP {
		for _, task := range m.mapTasks {
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
	} else if m.stage == REDUCE {
		for _, task := range m.reduceTasks {
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

	} else if m.stage == COMPLETE {
		reply.Status = COMPLETE
	}
	return nil
}

func (m *Master) UpdateTaskStatus(args *Task, reply *Task) error {
	if args.Status == COMPLETE {
		if args.Type == MAP {
			m.mapTasks[args.TaskID].Status = COMPLETE
			allComplete := true
			for _, task := range m.mapTasks {
				if task.Status != COMPLETE {
					allComplete = false
				}
			}
			if allComplete {
				log.Println("REDUCE TASKS ALL COMPLETE... SWITCHING TO REDUCE STAGE")
				m.stage = REDUCE
			}
		} else if args.Type == REDUCE {
			m.reduceTasks[args.TaskID].Status = COMPLETE
			allComplete := true
			for _, task := range m.reduceTasks {
				if task.Status != COMPLETE {
					allComplete = false
				}
			}
			if allComplete {
				log.Println("REDUCE TASKS ALL COMPLETE... SWITCHING TO COMPLETE STAGE")
				m.stage = COMPLETE
			}
		}
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if m.stage == COMPLETE {
		log.Println("Tasks complete... quitting.")
		return true
	}
	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.nReduce = nReduce
	m.stage = MAP
	m.mapTasks = make(map[int]*Task)
	m.reduceTasks = make(map[int]*Task)

	log.Println("Generating map tasks...")
	for id, file := range files {
		mapTask := Task{}
		mapTask.Type = MAP
		mapTask.Filepath = file
		mapTask.TaskID = id
		mapTask.Status = NOT_STARTED
		mapTask.NReduce = m.nReduce
		m.mapTasks[id] = &mapTask
		log.Printf("TASK #%d - Type: %v - Status: %v - Filepath: %v\n", mapTask.TaskID, mapTask.Type, mapTask.Status, mapTask.Filepath)
	}

	log.Println("Initializing reduce tasks...")
	for id:= 0; id < nReduce; id++ {
		reduceTask := Task{}
		reduceTask.Type = REDUCE
		reduceTask.TaskID = id
		reduceTask.Status = NOT_STARTED
		reduceTask.Filepath = "N/A"
		m.reduceTasks[id] = &reduceTask
		log.Printf("TASK #%d - Type: %v - Status: %v - Filepath: %v\n", reduceTask.TaskID, reduceTask.Type, reduceTask.Status, reduceTask.Filepath)
	}

	m.server()
	return &m
}
