package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskType int
const (
    TaskNone  = 0   // 没活，等着
    TaskMap   = 1   // map任务
    TaskReduce= 2   // reduce任务
    TaskExit  = -99 // 关门大吉，退出
)

type Empty struct{}


type TaskAssignment struct {

/*
in map we need x no y
in reduce we need y no x
X & Y here are universal
*/

	TaskType TaskType
	Filename string 
	LowerX int
	LowerY int 
	X int // # of files in total
	Y int // nReduce
	
} 

type TaskCompletion struct {
	TaskComplete bool
}




// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
