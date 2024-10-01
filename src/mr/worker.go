package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"sync"
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
			return fmt.Errorf("mapWrite: %w", err)
		}
	}
	return nil
}

func reduceWrite(newKVs []KeyValue, file *os.File) error {
	for _, newKV := range newKVs {
		line := fmt.Sprintf("%v %v\n", newKV.Key, newKV.Value)
		if _, err := file.WriteString(line); err != nil {
			return fmt.Errorf("reduceWrite: %w", err)
		}
	}
	return nil
}

/**
 * 单个 routine
 * 每次覆盖式地将结果写入到 mr-<task.id>-<reduceID> 中
 */
func doMap(task *MRTask, mapf func(string, string) []KeyValue, nReduce int) error {
	inputFile, err := os.Open(task.Filename)
	if err != nil {
		return fmt.Errorf("doMap: %w", err)
	}
	defer inputFile.Close()

	outputFiles := make([]*os.File, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		mapFileName := fmt.Sprintf("mr-%d-%d", task.ID, i)
		file, err := os.OpenFile(mapFileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("doMap: %w", err)
		}
		defer file.Close()
		outputFiles = append(outputFiles, file)
	}

	content, err := io.ReadAll(inputFile)
	if err != nil {
		return fmt.Errorf("doMap: %w", err)
	}

	newKVs := mapf(task.Filename, string(content))
	err = mapWrite(newKVs, nReduce, outputFiles)
	if err != nil {
		return fmt.Errorf("doMap: %w", err)
	}

	return nil
}

/*
 * reduce 阶段读单个文件
 */
func reduceReadFile(kvCache map[string][]string, fileName string) error {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("reduceReadFile: %w", err)
	}
	defer file.Close()

	var newKey, newVal string
	for {
		n, err := fmt.Fscanf(file, "%v %v", &newKey, &newVal)
		if err != nil && err != io.EOF {
			return fmt.Errorf("reduceReadFile: %w", err)
		}
		if n < 2 {
			return nil
		}

		if _, exist := kvCache[newKey]; !exist {
			kvCache[newKey] = make([]string, 0)
		}
		kvCache[newKey] = append(kvCache[newKey], newVal)
	}
}

/**
 * 多个 routine
 * 将所有 reduceID 匹配的 mr-*-<reduceID> 进行聚合、排序到 map[string][]string 中
 * 对每一个 key，启动一个 goroutine 来写入到 kvOutput，写入完成后进行排序
 * 最终将 kvOutput 写入到 mr-out-<reduceID> 中
 */
func doReduce(task *MRTask, reducef func(string, []string) string) error {
	// 初始化 kvCache
	kvCache := make(map[string][]string, 1024)

	// 读入 mr-out-*-<reduceID> 的文件至 kvCache
	pattern := fmt.Sprintf(`mr-\d+-%d`, task.ID)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("doReduce: %w", err)
	}

	files, err := os.ReadDir(".")
	if err != nil {
		return fmt.Errorf("doReduce: %w", err)
	}

	for _, file := range files {
		if !file.IsDir() && re.MatchString(file.Name()) {
			err := reduceReadFile(kvCache, file.Name())
			if err != nil {
				return fmt.Errorf("doReduce: %w", err)
			}
		}
	}

	// 打开 mr-out-<reduceID> 文件
	outputFileName := fmt.Sprintf("mr-out-%d", task.ID)
	outputFile, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("doReduce: %w", err)
	}
	defer outputFile.Close()

	// reducef 的结果写入首先无序地写入 kvOutput
	// 打开 outputChan 写通道
	kvOutput := make([]KeyValue, 0)
	writeChan := make(chan KeyValue, len(kvCache))

	writeWg := sync.WaitGroup{}
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		for msg := range writeChan {
			kvOutput = append(kvOutput, msg)
		}
	}()

	// reducef 后写入
	reduceWg := sync.WaitGroup{}
	for key, vals := range kvCache {
		reduceWg.Add(1)
		go func(key string, vals []string) {
			defer reduceWg.Done()
			kvElem := KeyValue{
				Key:   key,
				Value: reducef(key, vals),
			}
			writeChan <- kvElem
		}(key, vals)
	}
	reduceWg.Wait()  // 等待 channel 写入完成
	close(writeChan) // 关闭 channel
	writeWg.Wait()   // 等待 kvOutput 写入完成

	// kvOutput 排序
	sort.Slice(kvOutput, func(i, j int) bool {
		return kvOutput[i].Key < kvOutput[j].Key
	})

	// 写入 mr-out-* 文件
	reduceWrite(kvOutput, outputFile)

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 是否 log
	var logOpen bool = false

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
			if logOpen {
				log.Printf("Wait")
			}
			continue
		case "end":
			if logOpen {
				log.Printf("Worker end")
			}
			return
		case "map":
			// log
			if logOpen {
				PrintTask(newTask)
			}

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
			if logOpen {
				log.Printf("Submitted map task %d", newTask.ID)
			}
		case "reduce":
			// log
			if logOpen {
				PrintTask(newTask)
			}

			// handle
			err := doReduce(newTask, reducef)

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
			if logOpen {
				log.Printf("Submitted reduce task %d", newTask.ID)
			}
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
		return nil, fmt.Errorf("CallAllocTask error")
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
		return fmt.Errorf("CallSubmitTask error")
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
