package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
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
	//CallExample()
	task := CallForTask()
	for task.Mode != 2 {
		switch task.Mode {
		case 0:
			Mapping(task, mapf)
			CommitMapTask(&task)
		case 1:
			Reducing(task, reducef)
			CommitReduceTask(&task)
		case 3:
			time.Sleep(2 * time.Second)
		}
		task = CallForTask()
	}

}

func CallForTask() Task {
	args := ExampleArgs{}
	// declare a reply structure.
	reply := Task{}
	ok := call("Coordinator.DistributeTask", &args, &reply)
	if ok && reply.Success {
		switch reply.Mode {
		case 0:
			fmt.Printf("Task mode: map,")
			fmt.Printf("Task id: %v, ", reply.ID)
			fmt.Printf("get filename: %v\n", reply.FileName)
		case 1:
			fmt.Printf("Task mode: reduce,")
			fmt.Printf("Task id: %v\n", reply.ID)
		case 2:
			fmt.Printf("exit.\n")
		case 3:
			fmt.Printf("waiting.\n")
		}
	} else {
		fmt.Printf("callForTask failed!\n")
	}
	return reply
}

func Mapping(mapJob Task, mapf func(string, string) []KeyValue) {
	if !mapJob.Success {
		return
	}
	var intermediate []KeyValue
	filename := mapJob.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	var next [10][]KeyValue
	i := 0
	for i < len(intermediate) {
		reduceID := ihash(intermediate[i].Key) % 10
		next[reduceID] = append(next[reduceID], KeyValue{Key: intermediate[i].Key, Value: intermediate[i].Value})
		i++
	}
	files := [10]*os.File{}
	for i := 0; i < 10; i++ {
		oname := fmt.Sprintf("mr-%d-%d", mapJob.ID, i)
		files[i], _ = os.Create(oname)
		encoder := json.NewEncoder(files[i])
		err = encoder.Encode(next[i])
		if err != nil {
			fmt.Println("json error\n", err.Error())
		}
		files[i].Close()
	}
}

func Reducing(reduceJob Task, reducef func(string, []string) string) {
	if !reduceJob.Success {
		return
	}
	nReduce := reduceJob.ID
	nMap := reduceJob.Map
	var intermediate []KeyValue
	for i := 0; i < nMap; i++ {
		var temp []KeyValue
		filename := fmt.Sprintf("mr-%d-%d", i, nReduce)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&temp)
		intermediate = append(intermediate, temp...)
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", nReduce)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			fmt.Printf("file error : %v", oname)
		}
		i = j
	}
	ofile.Close()
}

func CommitMapTask(task *Task) bool {
	args := task
	reply := false
	ok := call("Coordinator.CheckMapTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("commit map task : %v\n", task.FileName)
	} else {
		fmt.Printf("callForTask failed!\n")
	}
	return reply
}

func CommitReduceTask(task *Task) bool {
	args := task
	reply := false
	ok := call("Coordinator.CheckReduceTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("commit reduce task : %v\n", task.ID)
	} else {
		fmt.Printf("callForTask failed!\n")
	}
	return reply
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
