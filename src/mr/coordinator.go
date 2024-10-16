package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type MRTaskStatus int

const (
	Unready MRTaskStatus = iota
	Ready
	Running
	Finished
)

type MRTask struct {
	Kind string // 类型: "map" | "reduce" | "end" | "wait"

	// map or reduce:
	ID     int          // 唯一标识 ID
	Status MRTaskStatus // 状态

	// map:
	Filename string // 输入文件

	// in mapTasks or reduceTasks
	// 在 Status == running 时有效
	StartTime time.Time
}

type coordinatorMessage struct {
	Kind string // "alloc" | "submit" | "heartbeat"

	// alloc:
	ReplyChan chan *MRTask

	// submit:
	TaskKind string
	ID       int
}

type Coordinator struct {
	mapTasks       []MRTask                // map Tasks
	mapTaskLeft    int                     // 剩余非 Finished
	reduceTasks    []MRTask                // reduce Tasks
	reduceTaskleft int                     // 剩余非 Finished
	messageChan    chan coordinatorMessage // 与 coordinatorHandler 通信
	nReduce        int                     // Reduce task 数量
}

func PrintTask(task *MRTask) {
	log.Printf("%+v\n", task)
}

func (c *Coordinator) PrintTasks() {
	for i := range c.mapTasks {
		log.Printf("map task %d: %+v\n", i, c.mapTasks[i])
	}
	for i := range c.reduceTasks {
		log.Printf("reduce task %d: %+v\n", i, c.reduceTasks[i])
	}
}

func (c *Coordinator) doAllocTask() *MRTask {
	if c.reduceTaskleft == 0 {
		return &MRTask{
			Kind: "end",
		}
	}

	if c.mapTaskLeft != 0 {
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == Ready {
				c.mapTasks[i].Status = Running
				c.mapTasks[i].StartTime = time.Now()
				return &c.mapTasks[i]
			}
		}

		// 所有 map 已经分配，需等待初始化 reduceTask 或 map 任务失败重分配
		return &MRTask{Kind: "wait"}
	} else {
		allocedNotFinished := false
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == Ready {
				c.reduceTasks[i].Status = Running
				c.reduceTasks[i].StartTime = time.Now()
				return &c.reduceTasks[i]
			} else if c.reduceTasks[i].Status == Running {
				// 只要有 Running 的 task 存在，
				// 即使全部分配，worker 也需要等待，
				// 因为 Running 的 worker 可能崩溃
				allocedNotFinished = true
			}
		}

		if allocedNotFinished {
			return &MRTask{Kind: "wait"}
		} else {
			// 所有 reduce 已经完成，闲置 worker 可以退出
			return &MRTask{Kind: "end"}
		}
	}
}

func (c *Coordinator) setReduceTasksReady() {
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i].Status = Ready
	}
}

func (c *Coordinator) doSubmitTask(TaskKind string, ID int) {
	switch TaskKind {
	case "map":
		if c.mapTasks[ID].Status == Running {
			c.mapTasks[ID].Status = Finished
			c.mapTaskLeft--
		}
		if c.mapTaskLeft == 0 {
			c.setReduceTasksReady()
		}
	case "reduce":
		if c.reduceTasks[ID].Status == Running {
			c.reduceTasks[ID].Status = Finished
			c.reduceTaskleft--
		}
	}
}

func (c *Coordinator) doHeartbeat() {
	if c.reduceTaskleft == 0 {
		return
	}
	if c.mapTaskLeft != 0 {
		for i := range c.mapTasks {
			if (c.mapTasks[i].Status == Running) && (time.Since(c.mapTasks[i].StartTime) > 10*time.Second) {
				c.mapTasks[i].Status = Ready
			}
		}
		return
	}
	if c.reduceTaskleft != 0 {
		for i := range c.reduceTasks {
			if (c.reduceTasks[i].Status == Running) && (time.Since(c.reduceTasks[i].StartTime) > 10*time.Second) {
				c.reduceTasks[i].Status = Ready
			}
		}
		return
	}
}

/**
 * 所有对 []task 数据结构的修改必须通过 channel -> handler
 * 保证并发安全性
 */
func (c *Coordinator) coordinatorHandler() {
	for msg := range c.messageChan {
		switch msg.Kind {
		case "alloc":
			msg.ReplyChan <- c.doAllocTask()

		case "submit":
			c.doSubmitTask(msg.TaskKind, msg.ID)

		case "heartbeat":
			c.doHeartbeat()
		}
	}
}

/**
 * 定时检查 running task 是否超时
 * 如果 running task 超时，发送消息将 status 重新设置为 Ready
 */
func (c *Coordinator) taskCrashChecker() {
	for {
		time.Sleep(time.Duration(10 * time.Second))
		var msg = coordinatorMessage{
			Kind: "heartbeat",
		}
		c.messageChan <- msg
	}
}

/*
 * RPC 函数
 */
func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) AllocMRTask(args *AllocTaskArgs, reply *AllocTaskReply) error {
	var ReplyChan = make(chan *MRTask)
	var allocMsg = coordinatorMessage{
		Kind:      "alloc",
		ReplyChan: ReplyChan,
	}

	c.messageChan <- allocMsg
	reply.Task = *<-ReplyChan
	close(ReplyChan)
	return nil
}

func (c *Coordinator) SubmitMRTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	var submitMsg = coordinatorMessage{
		Kind:     "submit",
		TaskKind: args.TaskKind,
		ID:       args.TaskID,
	}
	c.messageChan <- submitMsg
	return nil
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
// if the entire job has Finished.
func (c *Coordinator) Done() bool {
	return c.mapTaskLeft == 0 && c.reduceTaskleft == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 初始化 mapTasks
	c.mapTasks = make([]MRTask, 0, len(files))
	for idx, Filename := range files {
		newMapTask := MRTask{
			Kind:     "map",
			ID:       idx,
			Status:   Ready,
			Filename: Filename,
		}
		c.mapTasks = append(c.mapTasks, newMapTask)
	}
	c.mapTaskLeft = len(files)

	// 设置 nReduce
	c.nReduce = nReduce

	// 初始化 reduceTasks
	c.reduceTasks = make([]MRTask, 0, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		newReduceTask := MRTask{
			Kind:   "reduce",
			ID:     i,
			Status: Unready,
		}
		c.reduceTasks = append(c.reduceTasks, newReduceTask)
	}
	c.reduceTaskleft = nReduce

	// 初始化 messageChannel
	// 启动 coordinatorHandler 协程
	c.messageChan = make(chan coordinatorMessage, 200)
	go c.coordinatorHandler()

	// 启动 crashChecker
	go c.taskCrashChecker()

	c.server()
	return &c
}
