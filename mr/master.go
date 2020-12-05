package mr

import (
	"log"
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
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *TaskArgs, reply *Task) error {

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
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.nReduce = nReduce
	m.mapTasks = make(map[int]*Task)
	m.reduceTasks = make(map[int]*Task)

	for id, file := range files {
		mapTask := Task{}
		mapTask.Type = MAP
		mapTask.Filepath = file
		mapTask.TaskID = id
		mapTask.Status = NOT_STARTED
		m.mapTasks[id] = &mapTask
	}

	for id:= 0; id < nReduce; id++ {
		reduceTask := Task{}
		reduceTask.Type = REDUCE
		reduceTask.TaskID = id
		reduceTask.Status = NOT_STARTED
		m.reduceTasks[id] = &reduceTask
	}

	m.server()
	return &m
}
