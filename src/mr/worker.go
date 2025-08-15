package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// neet to establish method for sorting KeyValue by Key
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
	flag := true
	for flag {
		reply := GetTaskReply() // Get a task from the coordinator

		if reply.Done {
			fmt.Println("All tasks are done.")
			flag = false
			continue
		}

		if reply.MapJob != nil {
			// Process Map Job
			HandleMapJob(reply.MapJob, mapf)
		} else if reply.ReduceJob != nil {
			// Process Reduce Job
			HandleReduceJob(reply.ReduceJob, reducef)
		} else {
			// No job available, wait a bit before asking again
			time.Sleep(time.Second)
		}
	}
}

func HandleMapJob(mapJob *MapJob, mapf func(string, string) []KeyValue) {
	filename := mapJob.Filename
	reduceCount := mapJob.ReducerJobCnt
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content)) // get the key-value pairs from map function

	sort.Sort(ByKey(kva)) // sort by key
	//rules:
	// The map phase should divide the intermediate keys into buckets for nReduce reduce tasks,
	// where nReduce is the number of reduce tasks -- the argument that main/mrcoordinator.go
	// passes to MakeCoordinator(). Each mapper should create nReduce intermediate files for
	// consumption by the reduce tasks.

	// partition the key-value pairs into reduceCount partitions
	partitionedKva := make([][]KeyValue, reduceCount)

	for _, v := range kva {
		partitionKey := ihash(v.Key) % reduceCount
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}

	// create intermediate files for each partition
	// The worker implementation should put the output of the X'th reduce task in the file mr-out-X.
	// A mr-out-X file should contain one line per Reduce function output.
	// The line should be generated with the Go "%v %v" format, called with the key and value.
	// Have a look in main/mrsequential.go for the line commented "this is the correct format".
	// The test script will fail if your implementation deviates too much from this format.

	intermieateFiles := make([]string, reduceCount)
	for i := 0; i < reduceCount; i++ {
		intermieateFile := fmt.Sprintf("mr-%v-%v", mapJob.MapJobNumber, i)
		intermieateFiles[i] = intermieateFile
		ofile, _ := os.Create(intermieateFile)

		b, err := json.Marshal(partitionedKva[i])
		if err != nil {
			fmt.Println("Marshal error: ", err)
		}
		ofile.Write(b)

		ofile.Close()
	}
	ReportMapTaskReply := ReportMapTaskArgs{
		InputFile:        filename,
		IntermediateFile: intermieateFiles,
		Pid:              os.Getpid(),
	}
	ReportMapTask(ReportMapTaskReply) // send the report to the coordinator
}

func HandleReduceJob(reduceJob *ReduceJob, reducef func(string, []string) string) {
	files := reduceJob.IntermediateFiles

	intermediateData := []KeyValue{}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		var input []KeyValue

		err = json.Unmarshal(data, &input)
		if err != nil {
			log.Fatalf("cannot unmarshal %v", file)
		}
		intermediateData = append(intermediateData, input...)
	}
	sort.Sort(ByKey(intermediateData)) // sort by key

	outname := fmt.Sprintf("mr-out-%v", reduceJob.ReducerJobNumber)
	tempFile, err := os.CreateTemp(".", outname)
	if err != nil {
		log.Fatalf("cannot create %v", outname)
	}
	i := 0
	for i < len(intermediateData) {
		j := i + 1
		for j < len(intermediateData) && intermediateData[j].Key == intermediateData[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateData[k].Value)
		}
		output := reducef(intermediateData[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediateData[i].Key, output)

		i = j
	}
	os.Rename(tempFile.Name(), outname) // rename the file to mr-out-X

	ReportReduceTaskArgs := ReportReduceTaskArgs{
		Pid:          os.Getpid(),
		ReduceNumber: reduceJob.ReducerJobNumber,
	}
	ReportReduceTask(ReportReduceTaskArgs) // send the report to the coordinator
}

// call api
func GetTaskReply() RequestTaskReply {
	args := RequestTaskArgs{}
	args.Pid = os.Getpid() // get the process ID

	reply := RequestTaskReply{}

	call("Coordinator.RequestTask", &args, &reply) // send the RPC request to the coordinator

	return reply
}
func ReportMapTask(args ReportMapTaskArgs) {
	reply := ReportMapTaskReply{}
	call("Coordinator.ReportMapTask", &args, &reply) // send the RPC request to the coordinator
}
func ReportReduceTask(args ReportReduceTaskArgs) {
	reply := ReportReduceTaskReply{}
	call("Coordinator.ReportReduceTask", &args, &reply) // send the RPC request to the coordinator
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallRPC() {

// 	// declare an argument structure.
// 	args := InputArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := OutputReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
