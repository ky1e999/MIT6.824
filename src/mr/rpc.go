package mr

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// example to show how to declare the arguments
// and reply for an RPC.
type JobType int
type Job struct {
	JobType    JobType
	InputFile  []string
	JobId      int
	ReducerNum int
}

const (
	MapJob JobType = iota
	ReduceJob
	Wait
	Killing
)

type JobState int
type Phase int

const (
	Working JobState = iota
	Waiting
	Done
)
const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type JobArgs struct{}
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}
type JobContainer struct {
	JobMap map[int]*JobMetaInfo
}
type JobMetaInfo struct {
	Jobstate     JobState
	Jobadr       *Job
	JobStarttime time.Time
}

func GetJob() Job {
	args := JobArgs{}
	reply := Job{}
	get := call("Coordinator.PollJob", &args, &reply)
	if get {
		fmt.Println(reply)
	} else {
		fmt.Println("call failed!")
	}
	return reply
}
func callDone(job *Job) Job {
	args := job
	reply := Job{}
	done := call("Coordinator.JobDone", &args, &reply)
	if done {
		fmt.Println(reply)
	} else {
		fmt.Println("call failed!")
	}
	return reply
}
func callworking(job *Job) error {
	args := job
	reply := Job{}
	working := call("Coordinator.WorkingState", &args, &reply)
	if working {
		fmt.Println(reply)
	} else {
		fmt.Println("call failed!")
	}
	return nil
}

// Add your RPC definitions here.
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
