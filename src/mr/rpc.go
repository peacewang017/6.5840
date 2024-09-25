package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// worker programs <-- RPC --> coordinator program

// 字段必须大写
type GetNReduceArgs struct{}

type GetNReduceReply struct {
	NReduce int
}

type AllocTaskArgs struct{}

type AllocTaskReply struct {
	Task MRTask
}

type SubmitTaskArgs struct {
	TaskKind string
	TaskID   int
}

type SubmitTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
