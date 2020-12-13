package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			log.Println("ALL TASKS HAVE BEEN COMPLETED")
			break
		}

		log.Printf("RECEIVED TASK # %d - Filepath: %v - Status: %v - Type: %v\n", assignedTask.TaskID, assignedTask.Filepath, assignedTask.Status, assignedTask.Type)

		if assignedTask.Type == MAP {
			file, err := os.Open(assignedTask.Filepath)
			if err != nil {
				log.Fatalf("cannot open %v", assignedTask.Filepath)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", assignedTask.Filepath)
			}
			file.Close()
			intermediateKV := mapf(assignedTask.Filepath, string(content))
			intermediateFiles := make(map[int]*os.File)
			intermediateFilenames := []string{}

			for i := 0; i < assignedTask.NReduce; i++ {
				tmpFile, err := ioutil.TempFile("", "mr")
				if err != nil {
					log.Fatalln("Failed to create temp file")
				}
				intermediateFiles[i] = tmpFile
				intermediateFilenames = append(intermediateFilenames, tmpFile.Name())
			}

			fmt.Println(len(intermediateKV))
			for _, keyValue := range intermediateKV {
				reduceTaskNum := ihash(keyValue.Key) % assignedTask.NReduce
				file, ok := intermediateFiles[reduceTaskNum]
				if !ok {
					log.Fatalln("Failed to load temp file")
				}
				enc := json.NewEncoder(file)
				err := enc.Encode(&keyValue)
				if err != nil {
					fmt.Println("fatal error")
					log.Fatal(err)
				}
			}

			for i, file := range intermediateFiles {
				os.Rename(intermediateFilenames[i], fmt.Sprintf("mr-%d-%d", assignedTask.TaskID, i))
				intermediateFilenames[i] = fmt.Sprintf("mr-%d-%d", assignedTask.TaskID, i)
				file.Close()
			}

			assignedTask.Status = COMPLETE
			reply := Task{}
			if call("Master.UpdateTaskStatus", &assignedTask, &reply){
				log.Println("call to Master.UpdateTaskStatus was successful")
			} else {
				log.Println("master couldn't update task status")
			}

		} else if assignedTask.Type == REDUCE {
			filepaths, err := filepath.Glob("mr-*-" + strconv.Itoa(assignedTask.NReduce))
			if err != nil {
				log.Fatalln("Failed to find reduce files")
			}

			intermediateKV := []KeyValue{}

			for _, filepath := range filepaths {
				file, err := os.Open(filepath)
				if err != nil {
					log.Fatalf("cannot open %v", filepath)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediateKV = append(intermediateKV, kv)
				}
			}

			sort.Sort(ByKey(intermediateKV))
			ofile, err := ioutil.TempFile("", "mr-out-temp")

			i := 0
			for i < len(intermediateKV) {
				j := i + 1
				for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediateKV[k].Value)
				}
				output := reducef(intermediateKV[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediateKV[i].Key, output)

				i = j
			}
			ofile.Close()
			os.Rename("mr-out-temp", fmt.Sprintf("mr-out-%d", assignedTask.TaskID))

			assignedTask.Status = COMPLETE
			reply := Task{}
			if call("Master.UpdateTaskStatus", &assignedTask, &reply){
				log.Println("call to Master.UpdateTaskStatus was successful")
			} else {
				log.Println("master couldn't update task status")
			}
		}

	}

}

func AskForTask() (*Task, error) {
	args := Task{}
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
