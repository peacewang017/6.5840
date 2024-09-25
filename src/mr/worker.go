package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/*
 * 将 KVbuf 中的内容写入到打开的文件中
 */
func mapWrite(newKVs []KeyValue, nReduce int, files []*os.File) error {
	for _, newKV := range newKVs {
		line := fmt.Sprintf("%v %v\n", newKV.Key, newKV.Value)
		reduceID := ihash(newKV.Key) % nReduce
		if _, err := files[reduceID].WriteString(line); err != nil {
			return errors.New("mapWrite->" + err.Error())
		}
	}
	return nil
}

/**
 * 单个 routine
 * 每次覆盖式地将结果写入到 mrout-<task.id>-<reduceID> 中
 */
func doMap(task *MRTask, mapf func(string, string) []KeyValue, nReduce int) error {
	inputFile, err := os.Open(task.Filename)
	if err != nil {
		return errors.New("doMap->" + err.Error())
	}
	defer inputFile.Close()

	outputFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		mapFileName := fmt.Sprintf("mr-out-%d-%d", task.ID, i)
		file, err := os.OpenFile(mapFileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return errors.New("doMap->" + err.Error())
		}
		defer file.Close()
		outputFiles = append(outputFiles, file)
	}

	buf := make([]byte, 1024)
	for {
		n, err := inputFile.Read(buf)
		if err != nil && err != io.EOF {
			return errors.New("doMap->" + err.Error())
		}
		if n == 0 {
			break
		}

		newKVs := mapf(task.Filename, string(buf[0:n]))
		err = mapWrite(newKVs, nReduce, outputFiles)
		if err != nil {
			return errors.New("doMap->" + err.Error())
		}
	}
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 第一次 RPC call 初始化 nReduce
	var nReduce int = 0

	for {
		if nReduce == 0 {
			nReduce = CallGetNReduce()
		}

		newTask, err := CallAllocTask()
		if err != nil {
			// RPC error 被视为 coordinator 结束关闭
			return
		}

		switch newTask.Kind {
		case "wait":
			continue
		case "end":
			log.Printf("Worker end")
			return
		case "map":
			// handle
			err := doMap(newTask, mapf, nReduce)
			if err != nil {
				log.Print(err.Error())
				return
			}
			// submit
			err = CallSubmitTask(newTask.Kind, newTask.ID)
			if err != nil {
				log.Print(err.Error())
				return
			}
		case "reduce":
			// doReduce()
			// submit
		}
	}
}

func CallGetNReduce() int {
	args := GetNReduceArgs{}
	reply := GetNReduceReply{}
	call("Coordinator.GetNReduce", &args, &reply)
	return reply.NReduce
}

/**
 * error 表示 coordinator 已关闭
 * MRTask.kind == "wait"，表示等待
 * MRTask.kind == "end"，表示 worker 退出
 */
func CallAllocTask() (*MRTask, error) {
	args := AllocTaskArgs{}
	reply := AllocTaskReply{
		Task: MRTask{},
	}
	ok := call("Coordinator.AllocMRTask", &args, &reply)
	if ok {
		return &reply.Task, nil
	} else {
		return nil, errors.New("CallAllocTask error")
	}
}

// error 表示 RPC 过程出现问题
func CallSubmitTask(kind string, id int) error {
	args := SubmitTaskArgs{
		TaskKind: kind,
		TaskID:   id,
	}
	reply := SubmitTaskReply{}
	ok := call("Coordinator.SubmitMRTask", &args, &reply)
	if ok {
		return nil
	} else {
		return errors.New("CallSubmitTask error")
	}

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
