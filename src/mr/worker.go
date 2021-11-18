package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % ReduceJobTotal to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		response := GetUnfinishedTask()
		task := response.Task
		if task.TaskType != "" {
			//log.Printf("Get Task: %v", task)
		}

		// 1: Map Task
		// 2: Reduce Task
		if task.TaskType == "Map" {
			MapWorker(mapf, task.MapFile, task.Index, response.NReduce)
			FinishTask(task)
		} else if task.TaskType == "Reduce" {
			ReduceWorker(reducef, task.Index, task.ReduceFile)
			FinishTask(task)
		} else if task.TaskType == "Exit" {
			break
		} else if task.TaskType == "" {
			time.Sleep(time.Duration(5) * time.Second)
		}
	}
}

func MapWorker(mapf func(string, string) []KeyValue, filename string, index int, nReduce int) {
	// Read content from the input file.
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// Exec the mapf function and get the result.
	kva := mapf(filename, string(content))

	// Divided into NReduce buckets.

	// Init the kvPartitions firstly.
	var kvPartitions [][]KeyValue
	for i := 0; i < nReduce; i++ {
		kvPartitions = append(kvPartitions, []KeyValue{})
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		kvPartitions[index] = append(kvPartitions[index], kv)
	}

	// Write results to file.
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%v-%v", index, i)
		ofile, _ := os.Create(oname)
		json.NewEncoder(ofile).Encode(&kvPartitions[i])
	}
}

func ReduceWorker(reducef func(string, []string) string, index int, reduceFiles []string) {
	// Get intermediate results from file and put them to kvsMap.
	var kvsMap map[string][]string
	kvsMap = make(map[string][]string)

	for _, filename := range reduceFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		var kvs []KeyValue
		if err := dec.Decode(&kvs); err != nil {
			break
		}
		file.Close()

		for _, kv := range kvs {
			if kvsMap[kv.Key] == nil {
				kvsMap[kv.Key] = []string{kv.Value}
			} else {
				kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
			}
		}
	}

	// Write output to file.
	oname := fmt.Sprintf("mr-out-%v", index)
	ofile, _ := os.Create(oname)
	for key, values := range kvsMap {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	ofile.Close()
}

func GetUnfinishedTask() GetUnfinishedTaskReply {
	args := GetUnfinishedTaskArgs{}
	reply := GetUnfinishedTaskReply{}
	call("Coordinator.GetUnfinishedTask", &args, &reply)
	return reply
}

func FinishTask(task Task) {
	args := FinishTaskArgs{}
	args.Task = task
	args.FinishedAt = time.Now().Unix()
	reply := FinishTaskReply{}
	call("Coordinator.FinishTask", &args, &reply)
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
