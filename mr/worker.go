package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		assignedTask, err := AskForTask()

		if err != nil {
			log.Panicln("worker could not retrieve task from master")
		}

		if assignedTask.Status == COMPLETE {
			log.Println("tasks have all been completed")
			break
		}

		log.Println("Assigned task info:")
		log.Printf("ID: %d\nFilepath: %v\nStatus: %v\nType: %v\n", assignedTask.TaskID, assignedTask.Filepath, assignedTask.Status, assignedTask.Type)

		file, err := os.Open(assignedTask.Filepath)
		if err != nil {
			log.Fatalf("cannot open %v", assignedTask.Filepath)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", assignedTask.Filepath)
		}
		file.Close()

		if assignedTask.Type == MAP {
			intermediateKV := mapf(assignedTask.Filepath, string(content))
			intermediateFiles := make(map[int]*os.File)

			for _, keyValue := range intermediateKV {
				reduceTaskNum := ihash(keyValue.Key) % assignedTask.NReduce
				file, ok := intermediateFiles[reduceTaskNum]
				if !ok {
					filename := fmt.Sprintf("mr-%d-%d", assignedTask.TaskID, reduceTaskNum)
					file, err := os.Create(filename)
					if err != nil {
						log.Fatal(err)
					}
					intermediateFiles[reduceTaskNum] = file
				}
				enc := json.NewEncoder(file)
				err := enc.Encode(&keyValue)
				if err != nil {
					log.Fatal(err)
				}
			}

		} else if assignedTask.Type == REDUCE {

		}
	}



}

func AskForTask() (*Task, error) {
	args := TaskArgs{}
	reply := Task{}

	if call("Master.AssignTask", &args, &reply) {
		log.Println("call to Master.AssignTask was successful")
		return &reply, nil
	} else {
		return nil, errors.New("master couldn't assign task")
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
