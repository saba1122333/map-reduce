package mr

import (
	"os"
	"strconv"
	"time"
)

type TaskArgs struct {
	// Empty request; worker asks for a task assignment
}

type TaskReply struct {
	// TaskType is one of: "map", "reduce", "wait", or "done"
	TaskType string
	// TaskId is the zero-based index of the assigned task
	TaskId int
	// Filename is the input file for map tasks (empty for reduce)
	Filename string
	// NReduce is the total number of reduce tasks in this job
	NReduce int
	// NMap is the total number of map tasks in this job
	NMap      int
	StartTime time.Time
}

type TaskCompleteArgs struct {
	// TaskType is "map" or "reduce" for the completed task
	TaskType string
	// TaskId is the zero-based index of the completed task
	TaskId int
	// StartTime echoes when the worker began the task
	StartTime time.Time
	// EndTime indicates when the worker finished the task
	EndTime time.Time
}

type TaskCompleteReply struct {
	// Struggler indicates the worker should discard produced output
	Struggler bool
}

// coordinatorSock returns the coordinator's UNIX-domain socket path in /var/tmp.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
