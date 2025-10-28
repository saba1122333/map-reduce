package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"

	"time"
)

type ByKey []KeyValue

// ByKey implements sort.Interface to order KeyValue by Key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const waitTimeOut = 0

type KeyValue struct {
	Key   string
	Value string
}

// ihash deterministically maps a key to a positive int for reducer partitioning.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker runs the fetch/execute/report loop for map and reduce tasks.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {

		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			return
		}

		argsComplete := TaskCompleteArgs{}
		replyComplete := TaskCompleteReply{}
		argsComplete.StartTime = reply.StartTime

		switch reply.TaskType {
		case "wait":
			time.Sleep(waitTimeOut * time.Second)
			continue
		case "done":
			return
		case "map":
			handleMapTask(mapf, &reply, &argsComplete, &replyComplete)
		case "reduce":
			handleReduceTask(reducef, &reply, &argsComplete, &replyComplete)
		}

	}
}

// handleReduceTask executes a reduce task: read partitions, sort, reduce, write output.
func handleReduceTask(
	reducef func(string, []string) string,
	reply *TaskReply,
	argsComplete *TaskCompleteArgs,
	replyComplete *TaskCompleteReply,
) {
	var intermediate []KeyValue
	allDone := false
	callOk := false

	defer func() {
		if replyComplete.Struggler || !allDone || !callOk {
			oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
			os.Remove(oname)
		}
	}()

	for mapId := 0; mapId < reply.NMap; mapId++ {
		filename := fmt.Sprintf("mr-%d-%d", mapId, reply.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
	ofile, err := os.Create(oname)
	if err != nil {
		return
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

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return
		}
		i = j
	}
	ofile.Close()

	argsComplete.TaskType = "reduce"
	argsComplete.TaskId = reply.TaskId
	argsComplete.EndTime = time.Now()

	allDone = true
	callOk = true
	call("Coordinator.CompleteTask", argsComplete, replyComplete)
}

// handleMapTask executes a map task: run mapf, partition results, write intermediate files.
func handleMapTask(mapf func(string, string) []KeyValue, reply *TaskReply, argsComplete *TaskCompleteArgs, replyComplete *TaskCompleteReply) {
	createdFiles := make([]string, reply.NReduce)
	created := false
	callOk := false
	defer func() {
		if replyComplete.Struggler || !created || !callOk {
			cleanupMapFiles(reply.TaskId, reply.NReduce)
		}
	}()
	content, err := os.ReadFile(reply.Filename)
	if err != nil {
		return
	}

	kvs := mapf(reply.Filename, string(content))
	buckets := make([][]KeyValue, reply.NReduce)

	for _, kv := range kvs {
		reduceId := ihash(kv.Key) % reply.NReduce
		buckets[reduceId] = append(buckets[reduceId], kv)
	}

	for reduceId, bucket := range buckets {
		fileName := fmt.Sprintf("mr-%d-%d", reply.TaskId, reduceId)
		createdFiles[reduceId] = fileName

		err = writeIntermediateFile(fileName, bucket)

		if err != nil {
			return
		}
	}

	argsComplete.TaskType = reply.TaskType
	argsComplete.TaskId = reply.TaskId
	argsComplete.EndTime = time.Now()

	created = true
	callOk = true
	call("Coordinator.CompleteTask", argsComplete, replyComplete)

}

// writeIntermediateFile writes JSON-encoded KeyValue entries to a file.
func writeIntermediateFile(filename string, kvs []KeyValue) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			return err
		}
	}
	return nil
}

// cleanupMapFiles removes intermediate files for a given map task.
func cleanupMapFiles(taskId, nReduce int) {
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		filename := fmt.Sprintf("mr-%d-%d", taskId, reduceId)
		os.Remove(filename)
	}
}

// call invokes an RPC on the coordinator over a UNIX-domain socket.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
