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

// example to show how to declare the arguments
// and reply for an RPC.
type Jobs struct {
	Type              int
	WorkerNum         int
	InputFile         string
	IntermediateFiles []string
	ShouldStop        bool
}

type MapJob struct {
	Filename      string // name of the file to be processed
	MapJobNumber  int
	ReducerJobCnt int
}

type ReduceJob struct {
	IntermediateFiles []string // list of intermediate files to be processed
	ReducerJobNumber  int
}

// communication api
type RequestTaskArgs struct {
	Pid int
}

type RequestTaskReply struct {
	MapJob    *MapJob
	ReduceJob *ReduceJob
	Done      bool
}

type ReportMapTaskArgs struct {
	InputFile        string
	IntermediateFile []string
	Pid              int
}

type ReportMapTaskReply struct {
}

type ReportReduceTaskArgs struct {
	Pid          int
	ReduceNumber int
}

type ReportReduceTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
