package mr

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

var lk sync.Mutex

type Coordinator struct {
	// Your definitions here.
	MapChannel    chan *Job
	ReduceChannel chan *Job
	ReducerNum    int
	Currentid     int
	files         []string
	CurPhase      Phase
	jobContainer  JobContainer
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	lk.Lock()
	defer lk.Unlock()
	if c.CurPhase == AllDone {
		fmt.Println("All Jobs are finished")
		return true
	} else {
		return false
	}
}
func (j *JobContainer) receive(jobInfo *JobMetaInfo) {
	id := jobInfo.Jobadr.JobId
	j.JobMap[id] = jobInfo
}
func (c *Coordinator) MapJobInit(files []string) { //Making a map task
	for _, file := range files {
		id := c.generateId()
		job := Job{
			JobType:    MapJob,
			InputFile:  []string{file},
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}
		jobinfo := JobMetaInfo{
			Jobstate: Waiting,
			Jobadr:   &job,
		}
		c.jobContainer.receive(&jobinfo)
		fmt.Println("a map task is made:", job)
		c.MapChannel <- &job
	}
}
func reducefiles(reducenum int) []string {
	dir, _ := os.Getwd()
	var files []string
	var ret []string
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		files = append(files, info.Name())
		return nil
	})
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, strconv.Itoa(reducenum)) {
			ret = append(ret, file)
		}
	}
	return ret
}
func (c *Coordinator) ReduceTaskInit() { //Making a map task
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateId()
		job := Job{
			JobType:    ReduceJob,
			InputFile:  reducefiles(i),
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}
		jobinfo := JobMetaInfo{
			Jobstate: Waiting,
			Jobadr:   &job,
		}
		c.jobContainer.receive(&jobinfo)
		fmt.Println("a Reduce job is made:", job)
		c.ReduceChannel <- &job
	}
}
func (c *Coordinator) generateId() int {
	i := c.Currentid
	c.Currentid++
	return i
}
func (c *Coordinator) JobDone(args *Job, reply *Job) error {
	lk.Lock()
	defer lk.Unlock()
	info, _ := c.jobContainer.JobMap[args.JobId]
	info.Jobstate = Done
	if info.Jobadr.JobType == MapJob {
		fmt.Println("MapJob ", args.JobId, " is finished")
	} else if info.Jobadr.JobType == ReduceJob {
		fmt.Println("ReduceJob ", args.JobId, " is finished")
	}
	return nil
}
func (j *JobContainer) allreducejobsdone() bool {
	redundone := 0
	for _, t := range j.JobMap {
		if t.Jobadr.JobType == ReduceJob {
			if t.Jobstate != Done {
				redundone++
			}
		}
	}
	if redundone == 0 {
		return true
	} else {
		return false
	}
}
func (j *JobContainer) allmapjobsdone() bool {
	mapundone := 0
	for _, t := range j.JobMap {
		if t.Jobadr.JobType == MapJob {
			if t.Jobstate != Done {
				mapundone++
			}
		}
	}
	if mapundone == 0 {
		return true
	} else {
		return false
	}
}
func (c *Coordinator) WorkingState(args *Job, reply *Job) error {
	lk.Lock()
	defer lk.Unlock()
	info, _ := c.jobContainer.JobMap[args.JobId]
	info.Jobstate = Working
	return nil
}
func (c *Coordinator) PollJob(args *JobArgs, reply *Job) error {
	lk.Lock()
	defer lk.Unlock()
	switch c.CurPhase {
	case MapPhase:
		{
			if len(c.MapChannel) > 0 {
				*reply = *<-c.MapChannel
				c.jobContainer.JobMap[reply.JobId].JobStarttime = time.Now()
				c.jobContainer.JobMap[reply.JobId].Jobstate = Working
			} else {
				reply.JobType = Wait
				if c.jobContainer.allmapjobsdone() {
					c.ReduceTaskInit()
					c.CurPhase = ReducePhase
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceChannel) > 0 {
				*reply = *<-c.ReduceChannel
				c.jobContainer.JobMap[reply.JobId].JobStarttime = time.Now()
				c.jobContainer.JobMap[reply.JobId].Jobstate = Working
			} else {
				reply.JobType = Wait
				if c.jobContainer.allreducejobsdone() {
					c.CurPhase = AllDone
				}
			}
			return nil
		}
	default:
		{
			reply.JobType = Killing
		}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:         files,
		ReducerNum:    nReduce,
		Currentid:     0,
		CurPhase:      MapPhase,
		MapChannel:    make(chan *Job, len(files)),
		ReduceChannel: make(chan *Job, nReduce),
		jobContainer:  JobContainer{JobMap: make(map[int]*JobMetaInfo, len(files)+nReduce)},
	}
	c.MapJobInit(files)
	c.server()
	go c.Crash()
	return &c
}
func (c *Coordinator) Crash() {
	for {
		time.Sleep(time.Second * 2)
		lk.Lock()
		if c.CurPhase == AllDone {
			lk.Unlock()
			break
		}
		for _, v := range c.jobContainer.JobMap {
			if v.Jobstate == Working && time.Now().Sub(v.JobStarttime) > 5*time.Second {
				if v.Jobadr.JobType == MapJob {
					c.MapChannel <- v.Jobadr
					v.Jobstate = Waiting
				} else if v.Jobadr.JobType == ReduceJob {
					c.ReduceChannel <- v.Jobadr
					v.Jobstate = Waiting
				}
			}
		}
		lk.Unlock()
	}
}
