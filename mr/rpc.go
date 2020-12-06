package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

const (
	MAP = "MAP"
	REDUCE = "REDUCE"
	NOT_STARTED = "NOT_STARTED"
	IN_PROGRESS = "IN_PROGRESS"
	COMPLETE = "COMPLETE"
)

// Add your RPC definitions here.
type TaskArgs struct {
}

type Task struct {
	Filepath string
	Status string
	TimeAssigned time.Time
	Type string
	NReduce int
	TaskID int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
