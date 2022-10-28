package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	flag := true
	for flag {
		job := GetJob()
		switch job.JobType {
		case MapJob:
			{
				callworking(&job)
				DoMap(mapf, &job)
				callDone(&job)
			}
		case ReduceJob:
			{
				callworking(&job)
				DoReduce(reducef, &job)
				callDone(&job)
			}
		case Wait:
			{
				fmt.Println("All jobs are in progress, pls wait..")
				time.Sleep(time.Second)
			}
		case Killing:
			{
				fmt.Println("all job are killed!")
				flag = false
			}
		}
	}
}
func DoReduce(reducef func(string, []string) string, todo *Job) {
	intermediate := shuffle(todo.InputFile)
	oname := "mr-out-" + strconv.Itoa(todo.JobId)
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Println("failed to creat file")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}
func shuffle(files []string) []KeyValue {
	var ret []KeyValue
	for _, file := range files {
		fi, err := os.Open(file)
		if err != nil {
			fmt.Println("file open error")
		}
		var context KeyValue
		decoder := json.NewDecoder(fi)
		for {
			err := decoder.Decode(&context)
			if err != nil {
				break
			}
			ret = append(ret, context)
		}
		fi.Close()
	}
	sort.Sort(ByKey(ret))
	return ret
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
func DoMap(mapf func(string, string) []KeyValue, todo *Job) {
	var intermediate []KeyValue
	filename := todo.InputFile[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	context, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("connot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(context))
	hash := make([][]KeyValue, todo.ReducerNum)
	for _, kv := range intermediate {
		hash[ihash(kv.Key)%todo.ReducerNum] = append(hash[ihash(kv.Key)%todo.ReducerNum], kv)
	}
	for i := 0; i < todo.ReducerNum; i++ {
		fname := "mr-tmp-" + strconv.Itoa(todo.JobId) + "-" + strconv.Itoa(i)
		f1, _ := os.Create(fname)
		enc := json.NewEncoder(f1)
		for _, kv := range hash[i] {
			enc.Encode(kv)
		}
		f1.Close()
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
