package mr

import "fmt"
import "log"
import "net/rpc"
import "os"
import "io/ioutil"
import "hash/fnv"
import "sort"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
//state const
//
const IDLE int = 0
const INPROGRESS int = 1
const COMPLETED int = 2
const MAP int = 0
const REDUCE int = 1


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// workerLife := true
	// workerState := IDLE
	
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//CallExample()
	task, nReduce := ReqTask()
	doMap(mapf, task, nReduce)
	//TaskDone(id)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// the worker calls for requesting a task.
//
func ReqTask() (*Task, int){
	
	args := StateArgs{}

	args.State = IDLE

	reply := StateReply{}
	
	task := Task{}

	ok := call("Coordinator.ReqHandler", &args, &reply)
	if ok {
		fmt.Printf("Worker %v\n", reply.WorkerID)
		fmt.Printf("Task id %v\n", reply.FileID)
		fmt.Printf("task gotten %v %v\n", reply.Category, reply.File)
	} else {
		fmt.Printf("call failed!\n")
	}

	task.Category = reply.Category
	task.File = reply.File
	task.ID = reply.FileID
	task.State = IDLE
	return &task, reply.NReduce
}

//
// signal the coordinator that the task is done
//
func TaskDone(id string)  {
	

}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

//
// do map
//
func doMap(mapf func(string, string) []KeyValue, task *Task, nReduce int)  {
	intermediate := []KeyValue{}
	filename := task.File
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
	
	for _, kv := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", task.ID, ihash(kv.Key) % nReduce)
		ofile, err := os.OpenFile(oname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("cannot open %v", ofile)
		}
		enc := json.NewEncoder(ofile)
		errj := enc.Encode(&kv)
		if errj != nil {
			log.Fatalf("encoding error!")
		}
	}
}

