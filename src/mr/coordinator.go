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

type Task struct {
	TaskType   string // Map/Reduce/Exit
	Index      int
	Status     string // Unassigned/Running/Timeout/Finished
	StartAt    int64
	MapFile    string
	ReduceFile []string
}

type Coordinator struct {
	mu         sync.Mutex
	inputFiles []string
	nReduce    int
	mapTask    []Task
	reduceTask []Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) checkTaskFinished(taskType string) bool {
	c.mu.Lock()
	var value = true
	var tasks []Task
	if taskType == "Map" {
		tasks = c.mapTask
	} else {
		tasks = c.reduceTask
	}

	for _, task := range tasks {
		if task.Status != "Finished" {
			value = false
			break
		}
	}
	c.mu.Unlock()
	return value
}

func isFreeTask(task Task) bool {
	if task.Status == "Timeout" || task.Status == "Unassigned" {
		return true
	}
	if task.Status == "Running" && time.Now().Unix()-task.StartAt > 10 {
		return true
	}
	return false
}

func (c *Coordinator) getFreeTask(taskType string) Task {
	c.mu.Lock()
	var tasks []Task
	if taskType == "Map" {
		tasks = c.mapTask
	} else {
		tasks = c.reduceTask
	}

	for i := 0; i < len(tasks); i++ {
		if isFreeTask(tasks[i]) {
			tasks[i].StartAt = time.Now().Unix()
			tasks[i].Status = "Running"
			c.mu.Unlock()
			return tasks[i]
		}
	}

	c.mu.Unlock()
	return Task{}
}

func (c *Coordinator) setReturnTask(time int64, index int, taskType string) {
	c.mu.Lock()
	var task *Task
	if taskType == "Map" {
		task = &c.mapTask[index]
	} else {
		task = &c.reduceTask[index]
	}

	var status = "Finished"
	if time-task.StartAt > 10 {
		status = "Timeout"
	}
	//log.Printf("%v %v task finished", index, taskType)
	task.Status = status
	c.mu.Unlock()
}

func (c *Coordinator) getExitTask() Task {
	var task = Task{}
	task.TaskType = "Exit"
	return task
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.setReturnTask(args.FinishedAt, args.Task.Index, args.Task.TaskType)
	return nil
}

func (c *Coordinator) GetUnfinishedTask(args *GetUnfinishedTaskArgs, reply *GetUnfinishedTaskReply) error {
	if !c.checkTaskFinished("Map") {
		reply.Task = c.getFreeTask("Map")
		reply.NReduce = c.nReduce
	} else if !c.checkTaskFinished("Reduce") {
		reply.Task = c.getFreeTask("Reduce")
		reply.NReduce = c.nReduce
	} else {
		reply.Task = c.getExitTask()
	}

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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := c.checkTaskFinished("Reduce")

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.inputFiles = files
	for i := 0; i < len(files); i++ {
		mt := Task{}
		mt.MapFile = files[i]
		mt.Status = "Unassigned"
		mt.TaskType = "Map"
		mt.Index = i
		c.mapTask = append(c.mapTask, mt)
	}

	for i := 0; i < nReduce; i++ {
		rt := Task{}
		rt.Status = "Unassigned"
		rt.TaskType = "Reduce"
		rt.Index = i
		for j := 0; j < len(files); j++ {
			rt.ReduceFile = append(rt.ReduceFile, fmt.Sprintf("mr-%v-%v", j, i))
		}
		c.reduceTask = append(c.reduceTask, rt)
	}

	c.server()
	return &c
}
