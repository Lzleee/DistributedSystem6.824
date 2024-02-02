package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"


type Coordinator struct {
	// Your definitions here.
	Nonce int
	NReduce int
	Tasks []Task
	Workers []WorkerState
}

//
// Task parameter
//
type Task struct {
	File string
	Category int // map or reduce
	State int
	ID int
}

//
// Worker parameter
//
type WorkerState struct {
	ID int
	State int
	TaskID int
}

//
// lock
//
var mutex sync.Mutex


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReqHandler(args *StateArgs, reply *StateReply) error {

	var taskDone int = NEW
	if args.WorkerID == NEW {
		reply.WorkerID = c.Nonce
		fmt.Println(c.Nonce)
		c.Nonce += 1
	} else {
		reply.WorkerID = args.WorkerID
		taskDone = args.TaskID
	}
	
	reply.NReduce = c.NReduce
	reply.Category = c.Tasks[0].Category // the category is decided by the first element of task table
	task := c.TaskSche(taskDone)
	if task == nil {
		fmt.Println("MAPDONE")
		reply.File = ""
		reply.FileID = NEW
		return nil
	}
	fmt.Println(task.File)
	reply.File = task.File
	reply.FileID = task.ID

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Nonce = 0
	c.NReduce = nReduce
	c.Tasks = []Task{}
	for i, file := range files {
		task := Task{}
		task.Category = MAP
		task.File = file
		task.ID = i
		task.State = IDLE
		c.Tasks = append(c.Tasks, task)
	}

	c.server()
	return &c
}

//
//choose an idle file
//
func (c *Coordinator) TaskSche(lastTask int) *Task {
	mutex.Lock()
	var task *Task = nil

	if lastTask != NEW {
		c.Tasks[lastTask].State = COMPLETED
	}
	for i :=0; i < len(c.Tasks); i++ {
		task = &c.Tasks[i]
		if(task.State == IDLE){
			task.State = INPROGRESS
			break
		}
		task = nil
	}
	mutex.Unlock()
	return task
}