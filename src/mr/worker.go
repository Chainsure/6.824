package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// log.SetOutput(ioutil.Discard)
	client := prepareClient()
	mapperId := 1
	pid := os.Getpid()
	reducerCnt := getReducerCnt(client)
	mapStart := time.Now()
	log.Printf("Pid of worker is %v\n", pid)
	if reducerCnt == 0 {
		log.Fatalf("get invalid reducer count!\n")
	} else {
		log.Printf("reducer count is %v\n", reducerCnt)
	}
	for !mapDone(client) {
		filename := getMapFile(client, mapperId, pid)
		if filename == "" {
			log.Println("All the map tasks are dealing ...")
			time.Sleep(time.Millisecond * 200)
		} else {
			log.Printf("Mapping file %v with mapper %v of process %v\n", filename, mapperId, pid)
			mapTask(client, pid, mapperId, filename, mapf, reducerCnt)
			mapperId++
		}
	}
	mapDur := time.Since(mapStart)
	log.Printf("Process %v Map phase done! It took %v seconds to execute.\n", pid, mapDur)

	reduceStart := time.Now()
	reducerId := 0
	for !reduceDone(client) {
		reduceIdx := getReduceIdx(client, reducerId, pid)
		if reduceIdx == -1 {
			log.Printf("All the reduce tasks are dealing ...")
			time.Sleep(time.Millisecond * 200)
		} else {
			log.Printf("Reduce task %v with reducer %v of process %v\n", reduceIdx, reducerId, pid)
			reduceTask(client, pid, reducerId, reduceIdx, reducef, reducerCnt)
			reducerId++
		}
	}
	reduceDur := time.Since(reduceStart)
	log.Printf("Process %v reduce phase done! It took %v seconds to execute.\n", pid, reduceDur)
	removeTempFiles(pid)
	client.Close()
}

// ordinary function
func removeTempFiles(pid int) {
	subDirs, err := os.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}
	for _, sub := range subDirs {
		filename := sub.Name()
		pattern := "^mr-" + strconv.Itoa(pid) + "-[0-9]+-[0-9]+$"
		matched, err := regexp.MatchString(pattern, filename)
		if err != nil {
			log.Fatal(err)
		}
		if matched {
			os.Remove(filename)
		}
	}
}

func reduceTask(c *rpc.Client,
	pid int,
	reducerId int,
	reduceIdx int,
	reducef func(string, []string) string,
	nReduce int) {
	subDirs, err := os.ReadDir("./")
	if err != nil {
		updateFileReduceState(c, reduceIdx, Idle)
		log.Fatal(err)
	}
	files := []string{}
	for _, sub := range subDirs {
		if !sub.IsDir() {
			filename := sub.Name()
			pattern := "^mr-[0-9]+-[0-9]+-" + strconv.Itoa(reduceIdx-1) + "$"
			matched, err := regexp.MatchString(pattern, filename)
			if err != nil {
				updateFileReduceState(c, reduceIdx, Idle)
				log.Fatal(err)
			}
			if matched {
				files = append(files, filename)
			}
		}
	}
	intermediate := []KeyValue{}
	for _, filename := range files {
		infile, err := os.Open(filename)
		if err != nil {
			updateFileReduceState(c, reduceIdx, Idle)
			log.Fatal(err)
		}
		dec := json.NewDecoder(infile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		infile.Close()
	}
	sort.Sort(ByKey(intermediate))
	tmpFileName := "mr-out-" + strconv.Itoa(pid) + "-" + strconv.Itoa(reducerId) + "-" + strconv.Itoa(reduceIdx)
	ofile, _ := os.Create(tmpFileName)
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	currFileReducer := getFileReducer(c, reduceIdx)
	if currFileReducer.WorkId != reducerId || currFileReducer.Pid != pid {
		err := os.Remove(tmpFileName)
		if err != nil {
			log.Printf("Remove intermediate reduce file %v failed!\n", tmpFileName)
		}
	} else {
		updateFileReduceState(c, reduceIdx, Done)
		finalFileName := "mr-out-" + strconv.Itoa(reduceIdx-1)
		os.Rename(tmpFileName, finalFileName)
		log.Printf("Reduce work of %v is done by reducer id %v in process %v!\n", reduceIdx, reducerId, pid)
	}
}

func mapTask(c *rpc.Client,
	pid int,
	mapperId int,
	filename string,
	mapf func(string, string) []KeyValue,
	nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		updateFileMapState(c, filename, Idle)
		log.Fatalf("cannot open %v!\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		updateFileMapState(c, filename, Idle)
		log.Fatalf("cannot read %v\n", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	ofilesname := []string{}
	for i := 0; i < nReduce; i++ {
		filename := "mr-" + strconv.Itoa(pid) + "-" + strconv.Itoa(mapperId) + "-" + strconv.Itoa(i)
		ofilesname = append(ofilesname, filename)
	}
	ofiles := []*os.File{}
	for _, filename := range ofilesname {
		ofile, _ := os.Create(filename)
		ofiles = append(ofiles, ofile)
	}
	for _, kvpair := range kva {
		key := kvpair.Key
		ofileId := ihash(key) % nReduce
		enc := json.NewEncoder(ofiles[ofileId])
		err := enc.Encode(&kvpair)
		if err != nil {
			log.Printf("call Encoder failed at mapper: %v", mapperId)
		}
	}
	for _, ofile := range ofiles {
		ofile.Close()
	}
	currFileMapper := getFileMapper(c, filename)
	if currFileMapper.WorkId != mapperId || currFileMapper.Pid != pid {
		for _, ofilename := range ofilesname {
			err := os.Remove(ofilename)
			if err != nil {
				log.Printf("remove intermediate file %v failed!\n", ofilename)
			}
		}
	} else {
		log.Printf("mapper id: %v pid: %v updates file %v with state %v\n", mapperId, pid, filename, Done)
		updateFileMapState(c, filename, Done)
	}
}

// call coordiantor
func getFileReducer(c *rpc.Client, reduceIdx int) WorkerRepStruct {
	reducerRep := WorkerRepStruct{}
	ok := call(c, "Coordinator.GetFileReducer", reduceIdx, &reducerRep)
	if !ok {
		log.Printf("call getFileReducer failed!\n")
		return WorkerRepStruct{}
	}
	return reducerRep
}

func updateFileReduceState(c *rpc.Client, reduceIdx int, state int) {
	log.Printf("update reduce task %v to state %v\n", reduceIdx, state)
	fileReduceState := FileReduceStatePair{
		ReduceIdx: reduceIdx,
		State:     state,
	}
	ok := callWithoutReplies(c, "Coordinator.UpdateFileReduceState", &fileReduceState)
	if !ok {
		log.Println("call updateFileReduceState failed!")
	}
}

func getReduceIdx(c *rpc.Client, reducerId int, pid int) int {
	reduceIdx := 0
	reducerRep := WorkerRepStruct{
		WorkId: reducerId,
		Pid:    pid,
	}
	ok := call(c, "Coordinator.GetReduceIdx", reducerRep, &reduceIdx)
	if !ok {
		log.Printf("call getReduceIdx failed")
		return -1
	}
	return reduceIdx
}

func reduceDone(c *rpc.Client) bool {
	done := false
	ok := callWithoutArgs(c, "Coordinator.ReduceDone", &done)
	if !ok {
		log.Printf("call reduceDone failed!")
		return false
	}
	return done
}

func getFileMapper(c *rpc.Client, filename string) WorkerRepStruct {
	mapperRep := WorkerRepStruct{}
	ok := call(c, "Coordinator.GetFileMapper", filename, &mapperRep)
	if !ok {
		log.Printf("call getFileMapper failed!\n")
		return WorkerRepStruct{}
	}
	return mapperRep
}

func getReducerCnt(c *rpc.Client) int {
	reducerCnt := 0
	ok := callWithoutArgs(c, "Coordinator.GetReducerCnt", &reducerCnt)
	if !ok {
		log.Printf("call getReducerCnt failed!\n")
		return 0
	}
	return reducerCnt
}

func mapDone(c *rpc.Client) bool {
	done := false
	ok := callWithoutArgs(c, "Coordinator.MapDone", &done)
	if !ok {
		log.Printf("call mapNotDone failed!\n")
		return false
	}
	return done
}

func getMapFile(c *rpc.Client, mapperId int, pid int) string {
	filename := ""
	mapperRep := WorkerRepStruct{
		Pid:    pid,
		WorkId: mapperId,
	}
	ok := call(c, "Coordinator.GetMapFile", &mapperRep, &filename)
	if !ok {
		log.Printf("call getMapFile failed!\n")
		return ""
	}
	return filename
}

func updateFileMapState(c *rpc.Client, filename string, state int) {
	fileMapState := FileMapStatePair{
		FileName: filename,
		State:    state,
	}
	ok := callWithoutReplies(c, "Coordinator.UpdateFileMapState", &fileMapState)
	if !ok {
		log.Printf("call updateFileMapState failed!\n")
	}
}

// rpc call
func prepareClient() *rpc.Client {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return c
}

func dealErrAfterCall(err error) bool {
	if err == nil {
		return true
	}
	log.Println(err)
	return false
}

func callWithoutArgs(c *rpc.Client, rpcname string, reply interface{}) bool {
	err := c.Call(rpcname, false, reply)
	return dealErrAfterCall(err)
}

func callWithoutReplies(c *rpc.Client, rpcname string, args interface{}) bool {
	err := c.Call(rpcname, args, nil)
	return dealErrAfterCall(err)
}

func call(c *rpc.Client, rpcname string, args interface{}, reply interface{}) bool {
	err := c.Call(rpcname, args, reply)
	return dealErrAfterCall(err)
}
