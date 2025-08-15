package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type JobStatus struct {
	StartTime int64
	Status    string
}

type Coordinator struct {
	// Your definitions here.
	workerStatus      map[int]string
	mapStatus         map[string]JobStatus // map of file names to their map task state
	mapTaskNumber     int                  // current map task ID
	reduceStatus      map[int]JobStatus    // map of reduce task IDs to their state
	nReducer          int                  // number of reduce tasks
	intermediateFiles map[int][]string     // location to store intermediate files
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.



func (c *Coordinator) ReportMapTask(args *ReportMapTaskArgs, reply *ReportMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//release the worker
	c.mapStatus[args.InputFile] = JobStatus{
		StartTime: -1, // -1 means completed
		Status:    "completed",
	}
	c.workerStatus[args.Pid] = "idle"
	// store the intermediate files
	for i := 0; i < c.nReducer; i++ {
		c.intermediateFiles[i] = append(c.intermediateFiles[i], args.IntermediateFile[i])
	}
	return nil

}

func (c *Coordinator) ReportReduceTask(args *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//release the worker
	c.reduceStatus[args.ReduceNumber] = JobStatus{
		StartTime: -1, // -1 means completed
		Status:    "completed",
	}
	c.workerStatus[args.Pid] = "idle"
	return nil
}

func (c *Coordinator) PickMapJob() *MapJob {
	// Find the first pending map job
	var mapJob *MapJob = nil
	for filename, status := range c.mapStatus {
		if status.Status == "pending" {
			mapJob = &MapJob{
				Filename:      filename,
				MapJobNumber:  c.mapTaskNumber,
				ReducerJobCnt: c.nReducer,
			}
			c.mapStatus[filename] = JobStatus{StartTime: time.Now().Unix(), Status: "running"}
			c.mapTaskNumber++
			break
		}
	}
	return mapJob

}

func (c *Coordinator) PickReduceJob() *ReduceJob {
	// Find the first pending reduce job
	var reduceJob *ReduceJob = nil
	reducer := -1 // -1 means no pending reduce job

	for i, status := range c.reduceStatus {
		if status.Status == "pending" {
			reducer = i
			break
		}
	}

	if reducer < 0 {
		return nil
	}

	// create a reduce job
	reduceJob = &ReduceJob{
		IntermediateFiles: c.intermediateFiles[reducer],
		ReducerJobNumber:  reducer,
	}
	c.reduceStatus[reducer] = JobStatus{StartTime: time.Now().Unix(), Status: "running"}

	return reduceJob
}

func (c *Coordinator) AllMapJobsDone() bool {
	var done bool = true
	for _, status := range c.mapStatus {
		if status.Status != "completed" {
			done = false
			break
		}
	}
	return done
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

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
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, status := range c.reduceStatus {
		if status.Status != "completed" {
			return false
		}
	}
	return true
}

// start a ticker that calls Done() periodically
func (c *Coordinator) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.CheckDeadWorker()
			}
		}
	}()
}

func (c *Coordinator) CheckDeadWorker() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.mapStatus {
		if v.Status == "running" {
			now := time.Now().Unix()
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.mapStatus[k] = JobStatus{StartTime: -1, Status: "pending"}
				continue
			}
		}
	}

	for k, v := range c.reduceStatus {
		if v.Status == "running" {
			now := time.Now().Unix()
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.reduceStatus[k] = JobStatus{StartTime: -1, Status: "pending"}
				continue
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// initialize the coordinator
	c.mapTaskNumber = 0
	c.nReducer = nReduce

	c.mapStatus = make(map[string]JobStatus)
	for _, file := range files {
		c.mapStatus[file] = JobStatus{StartTime: -1, Status: "pending"}
	}

	c.reduceStatus = make(map[int]JobStatus)
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = JobStatus{StartTime: -1, Status: "pending"}
	}
	c.workerStatus = make(map[int]string)
	c.intermediateFiles = make(map[int][]string)
	c.StartTicker()
	c.server()
	return &c
}
