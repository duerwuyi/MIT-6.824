package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	nReduce     int
	nMap        int
	mapState    []bool
	reduceState []bool
	State       int
	mapChan     chan MapTask
	reduceChan  chan ReduceTask
	stateLock   sync.Mutex
}

func (c *Coordinator) getState() int {
	c.stateLock.Lock()
	i := c.State
	c.stateLock.Unlock()
	return i
}

func (c *Coordinator) setState(i int) {
	c.stateLock.Lock()
	c.State = i
	c.stateLock.Unlock()
	return
}

type ReduceTask struct {
	TaskID int
}

type MapTask struct {
	TaskID   int
	FileName string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *Task) error {
	c.stateLock.Lock()
	reply.Mode = c.State
	reply.Map = c.nMap
	reply.Reduce = c.nReduce
	reply.Success = true
	if c.State == 0 {
		select {
		case mapWork := <-c.mapChan:
			fmt.Printf("send map work:%v\n", mapWork.FileName)
			reply.FileName = mapWork.FileName
			reply.ID = mapWork.TaskID
			go c.mapTimer(mapWork)
		default:
			fmt.Printf("waiting for state shifting to 1.\n")
			reply.Mode = 3
		}
	} else if c.State == 1 {
		select {
		case reduceWork := <-c.reduceChan:
			fmt.Printf("send reduce work:%v\n", reduceWork.TaskID)
			reply.ID = reduceWork.TaskID
			reply.FileName = ""
			go c.reduceTimer(reduceWork)
		default:
			fmt.Printf("waiting for state shifting to 2.\n")
			reply.Mode = 3
		}
	}
	c.stateLock.Unlock()
	return nil
}

func (c *Coordinator) CheckMapTask(args *Task, reply *bool) error {
	c.stateLock.Lock()
	fmt.Printf("check map task:%d. ", args.ID)
	fmt.Printf("filename:%v\n", args.FileName)
	c.mapState[args.ID] = true
	if c.checkMapAll() {
		c.State = 1
	}
	*reply = true
	c.stateLock.Unlock()
	return nil
}

func (c *Coordinator) checkMapAll() bool {
	for _, done := range c.mapState {
		if !done {
			return false
		}
	}
	return true
}

func (c *Coordinator) CheckReduceTask(args *Task, reply *bool) error {
	c.stateLock.Lock()
	c.reduceState[args.ID] = true
	if c.checkReduceAll() {
		c.State = 2
	}
	*reply = true
	c.stateLock.Unlock()
	return nil
}

func (c *Coordinator) checkReduceAll() bool {
	for _, done := range c.reduceState {
		if !done {
			return false
		}
	}
	return true
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
	ret := c.getState()

	// Your code here.

	return ret == 2
}

func (c *Coordinator) mapTimer(mapTask MapTask) {
	time.Sleep(10 * time.Second)
	c.stateLock.Lock()
	done := c.mapState[mapTask.TaskID]
	if !done {
		c.mapChan <- mapTask
	}
	c.stateLock.Unlock()
}

func (c *Coordinator) reduceTimer(reduceTask ReduceTask) {
	time.Sleep(10 * time.Second)
	c.stateLock.Lock()
	done := c.reduceState[reduceTask.TaskID]
	if !done {
		c.reduceChan <- reduceTask
	}
	c.stateLock.Unlock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapState = make([]bool, c.nMap)
	c.reduceState = make([]bool, c.nReduce)
	c.mapChan = make(chan MapTask, c.nMap)
	c.reduceChan = make(chan ReduceTask, c.nReduce)
	c.State = 0
	fmt.Print("coordinator started.\n")
	for i, filename := range files {
		mapTask := MapTask{TaskID: i, FileName: filename}
		c.mapChan <- mapTask
	}
	for j := 0; j < nReduce; j++ {
		c.reduceChan <- ReduceTask{TaskID: j}
	}
	c.server()
	return &c
}
