package mr

//
// RPC definitions.
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

type Task struct {
	Filepath string
	Status string
	TimeAssigned time.Time
	Type string
	NReduce int
	TaskID int
}

// 'masterSock' function provided in starter code:
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
