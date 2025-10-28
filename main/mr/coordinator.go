package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskTime              = 3
	stalenessCheckTimeOut = 10
)

type Coordinator struct {
	// mapTasks holds status/timing for each map task by index
	mapTasks []TaskStatus
	// reduceTasks holds status/timing for each reduce task by index
	reduceTasks []TaskStatus
	// files are input filenames aligned to map task indices
	files []string
	// nReduce is total number of reduce tasks
	nReduce int
	// nMap is total number of map tasks
	nMap int
	// mu guards Coordinator state across RPCs/goroutines
	mu sync.Mutex
	// cleanedUp indicates intermediate files were removed once
	cleanedUp bool
}

type TaskStatus struct {
	// Status is one of: "pending", "in progress", "done"
	Status string
	// StartTime when a worker started this task
	StartTime time.Time
	// EndTime when a worker reported completion
	EndTime time.Time
}

// CompleteTask marks a reported map/reduce task as done.
func (c *Coordinator) CompleteTask(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case "map":
		c.mapTasks[args.TaskId].Status = "done"
		c.mapTasks[args.TaskId].EndTime = args.EndTime
	case "reduce":
		c.reduceTasks[args.TaskId].Status = "done"
		c.reduceTasks[args.TaskId].EndTime = args.EndTime

	}

	return nil
}

// AssignTask returns a pending task, a wait signal, or done when all complete.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	allMapDone := true
	allReduceDone := true
	for _, mtask := range c.mapTasks {
		if mtask.Status != "done" {
			allMapDone = false
			break
		}
	}
	for _, mtask := range c.reduceTasks {
		if mtask.Status != "done" {
			allReduceDone = false
			break
		}
	}

	if allMapDone && allReduceDone {
		reply.TaskType = "done"
		return nil

	}

	allMapAssigned := true
	allReduceAssigned := true

	for _, mtask := range c.mapTasks {
		if mtask.Status != "in progress" {
			allMapAssigned = false
			break
		}
	}

	for _, mtask := range c.reduceTasks {
		if mtask.Status != "in progress" {
			allReduceAssigned = false
			break
		}
	}

	if allMapAssigned || allReduceAssigned {
		reply.TaskType = "wait"
		return nil
	}

	for tid, mtask := range c.mapTasks {
		if mtask.Status == "pending" {
			reply.TaskType = "map"
			reply.TaskId = tid
			reply.Filename = c.files[tid]
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			startTime := time.Now()
			reply.StartTime = startTime
			c.mapTasks[tid].StartTime = startTime
			c.mapTasks[tid].Status = "in progress"

			return nil
		}
	}
	if allMapDone {
		for tid, mtask := range c.reduceTasks {
			if mtask.Status == "pending" {
				reply.TaskType = "reduce"
				reply.TaskId = tid
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				startTime := time.Now()
				reply.StartTime = startTime
				c.reduceTasks[tid].StartTime = startTime
				c.reduceTasks[tid].Status = "in progress"

				return nil
			}
		}
	}

	return nil
}

// detectStaleTasks re-queues tasks stuck in progress beyond TaskTime.
func (c *Coordinator) detectStaleTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	timeout := TaskTime * time.Second

	for i := range c.mapTasks {
		if c.mapTasks[i].Status == "in progress" {
			elapsed := time.Since(c.mapTasks[i].StartTime)
			if elapsed > timeout {
				c.mapTasks[i].Status = "pending"
			}
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status == "in progress" {
			elapsed := time.Since(c.reduceTasks[i].StartTime)
			if elapsed > timeout {
				c.reduceTasks[i].Status = "pending"
			}
		}
	}
}

// server exposes Coordinator RPC endpoints over a UNIX socket.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done reports whether all map and reduce tasks are finished; cleans up once.
func (c *Coordinator) Done() bool {
	for _, v := range c.mapTasks {
		if v.Status != "done" {
			return false
		}
	}
	for _, v := range c.reduceTasks {
		if v.Status != "done" {
			return false
		}
	}
	if !c.cleanedUp {
		c.cleanupMapFiles()
		c.cleanedUp = true
	}
	return true
}

// cleanupMapFiles deletes mr-MAP-REDUCE intermediate files.
func (c *Coordinator) cleanupMapFiles() {
	for taskId := 0; taskId < len(c.mapTasks); taskId++ {
		for reduceId := 0; reduceId < c.nReduce; reduceId++ {
			filename := fmt.Sprintf("mr-%d-%d", taskId, reduceId)
			os.Remove(filename)
		}
	}
}

// MakeCoordinator initializes Coordinator state and starts serving.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]TaskStatus, len(files))
	reduceTasks := make([]TaskStatus, nReduce)
	for i := range files {
		mapTasks[i] = TaskStatus{
			Status: "pending",
		}

	}
	for i := range reduceTasks {
		reduceTasks[i] = TaskStatus{
			Status: "pending",
		}
	}
	c := Coordinator{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		files:       files,
		nMap:        len(files),
		nReduce:     nReduce,
		mu:          sync.Mutex{}}

	c.server()
	go func() {
		for !c.Done() {
			time.Sleep(stalenessCheckTimeOut * time.Second)
			c.detectStaleTasks()
		}
	}()
	return &c
}
