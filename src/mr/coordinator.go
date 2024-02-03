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
	State int
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
	task := c.TaskSche(taskDone)
	reply.Category = c.Tasks[0].Category // the category is decided by the first element of task table
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
	if c.State == COMPLETED {
		ret = true
	}

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
	c.State = INPROGRESS
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
// bug!!!: A thread terminates map tasks with an id, the the task mode changes. 
// But the thread does not realise that the mode is change. 
// It make the reduce task table dirty and the corresponding id is marked as COMPLETED.
//
func (c *Coordinator) TaskSche(lastTask int) *Task {
	mutex.Lock()
	var task *Task = nil
	allDone := false

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
	for i :=0; i < len(c.Tasks); i++ {
		taskTep := &c.Tasks[i]
		if(taskTep.State != COMPLETED){
			break
		}
		if i == len(c.Tasks) - 1 {
			allDone = true
		}
	}

	if task == nil {
		if allDone {
			if c.Tasks[0].Category == MAP {
				fmt.Println("MAPDONE")
				c.changeMode()
				for i :=0; i < len(c.Tasks); i++ {
					task = &c.Tasks[i]
					if(task.State == IDLE){
						task.State = INPROGRESS
						break
					}
					task = nil
				}
			}else {
				fmt.Println("DONE")
				c.State = COMPLETED
				task = &Task{}
				task.ID = NEW
				task.File = ""
				task.Category = REDUCE
				task.State = COMPLETED
			}
		} else {
			task = &Task{}
			task.Category = MAP
			task.File = ""
			task.ID = NEW
			task.State = IDLE
		}
	}
	mutex.Unlock()
	return task
}

//
// change task mode
//
func (c *Coordinator) changeMode()  {
	tasks := []Task{}
	for i := 0; i < c.NReduce; i++ {
		task := Task{}
		task.Category = REDUCE
		task.File = fmt.Sprintf("mr-*-%v", i)
		task.State = IDLE
		task.ID = i
		tasks = append(tasks, task)
	}
	c.Tasks = tasks
}