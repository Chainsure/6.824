package mr

import (
	"os"
	"strconv"
)

// rpc definitions
type Int32Struct struct {
	Int32Val int
}

type WorkerRepStruct struct {
	Pid    int
	WorkId int
}

type FileMapStatePair struct {
	FileName string
	State    int
}

type FileReduceStatePair struct {
	ReduceIdx int
	State     int
}

type FileName struct {
	File string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
