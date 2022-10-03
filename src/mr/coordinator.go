package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Idle       = iota
	Processing = iota
	Done       = iota
)

type Coordinator struct {
	files         []string
	reducerCount  int
	mapdone       bool
	reducedone    bool
	mapperStateMu sync.Mutex
	mapperTimerMu sync.Mutex
	mapperIdMu    sync.Mutex
	mapDoneMu     sync.Mutex
	reduceStateMu sync.Mutex
	reduceTimerMu sync.Mutex
	reduceIdMu    sync.Mutex
	reduceDoneMu  sync.Mutex
	mapperState   map[string]int
	mapperTimer   map[string]int
	mapperId      map[string]WorkerRepStruct
	reducerState  map[int]int
	reducerTimer  map[int]int
	reducerId     map[int]WorkerRepStruct
}

// ordinary function
func (c *Coordinator) checkMapTimer() {
	for {
		time.Sleep(time.Millisecond * 10)
		allDone := true
		c.mapperStateMu.Lock()
		for file, state := range c.mapperState {
			if state == Processing {
				allDone = false
				c.mapperTimerMu.Lock()
				c.mapperTimer[file] -= 10
				if c.mapperTimer[file] <= 0 {
					c.mapperState[file] = Idle
					c.mapperIdMu.Lock()
					c.mapperId[file] = WorkerRepStruct{}
					c.mapperIdMu.Unlock()
				}
				c.mapperTimerMu.Unlock()
			} else if state == Idle {
				allDone = false
			}
		}
		c.mapperStateMu.Unlock()
		if allDone {
			break
		}
	}
	for {
		allDone := true
		c.reduceStateMu.Lock()
		for id, state := range c.reducerState {
			time.Sleep(time.Millisecond * 10)
			if state == Processing {
				allDone = false
				c.reduceTimerMu.Lock()
				c.reducerTimer[id] -= 10
				if c.reducerTimer[id] <= 0 {
					c.reducerState[id] = Idle
					c.reduceIdMu.Lock()
					c.reducerId[id] = WorkerRepStruct{}
					c.reduceIdMu.Unlock()
				}
				c.reduceTimerMu.Unlock()
			} else if state == Idle {
				allDone = false
			}
		}
		c.reduceStateMu.Unlock()
		if allDone {
			break
		}
	}
}

// rpc call method
func (c *Coordinator) GetFileReducer(reduceIdx int, reducerRep *WorkerRepStruct) error {
	c.reduceIdMu.Lock()
	rep, ok := c.reducerId[reduceIdx]
	c.reduceIdMu.Unlock()
	if !ok {
		err := fmt.Errorf("%v does not exists in reduceIdMap", reduceIdx)
		return err
	} else {
		*reducerRep = rep
	}
	return nil
}

func (c *Coordinator) UpdateFileReduceState(filestate *FileReduceStatePair, reply *bool) error {
	reduceIdx, state := filestate.ReduceIdx, filestate.State
	c.reduceStateMu.Lock()
	prevState, ok := c.reducerState[reduceIdx]
	if !ok {
		c.reduceStateMu.Unlock()
		err := fmt.Errorf("%v does not exist in reduceStateMap", reduceIdx)
		return err
	} else {
		log.Printf("Update reduce task %v reduce state to %v\n", reduceIdx, state)
		if prevState != state {
			c.reducerState[reduceIdx] = state
		}
		c.reduceStateMu.Unlock()
	}
	return nil
}

func (c *Coordinator) GetReduceIdx(reducerRep *WorkerRepStruct, reduceIdx *int) error {
	*reduceIdx = -1
	c.reduceStateMu.Lock()
	defer c.reduceStateMu.Unlock()
	for i := 1; i <= c.reducerCount; i++ {
		if c.reducerState[i] == Idle {
			c.reduceTimerMu.Lock()
			c.reducerTimer[i] = 10 * 1000
			c.reduceTimerMu.Unlock()
			c.reduceIdMu.Lock()
			c.reducerId[i] = *reducerRep
			c.reduceIdMu.Unlock()
			c.reducerState[i] = Processing
			*reduceIdx = i
			log.Printf("return reduce idx %v\n", i)
			break
		}
	}
	return nil
}

func (c *Coordinator) ReduceDone(args bool, reduceDone *Int32Struct) error {
	done := 1
	c.reduceStateMu.Lock()
	for _, reduceState := range c.reducerState {
		if reduceState != Done {
			done = 0
			break
		}
	}
	c.reduceStateMu.Unlock()
	reduceDone.Int32Val = done
	if done == 1 {
		c.reduceDoneMu.Lock()
		c.reducedone = true
		c.reduceDoneMu.Unlock()
	}
	return nil
}

func (c *Coordinator) GetFileMapper(filename *FileName, mapperRep *WorkerRepStruct) error {
	c.mapperIdMu.Lock()
	rep, ok := c.mapperId[filename.File]
	c.mapperIdMu.Unlock()
	if !ok {
		log.Printf("cannot find file %v in mapperId\n", filename.File)
	} else {
		*mapperRep = rep
	}
	return nil
}

func (c *Coordinator) GetReducerCnt(arg bool, reducerCnt *int) error {
	*reducerCnt = c.reducerCount
	return nil
}

func (c *Coordinator) GetMapFile(mapperRep *WorkerRepStruct, filename *FileName) error {
	c.mapperStateMu.Lock()
	defer c.mapperStateMu.Unlock()
	for file, state := range c.mapperState {
		if state == Idle {
			filename.File = file
			c.mapperState[file] = Processing
			c.mapperTimerMu.Lock()
			c.mapperTimer[filename.File] = 10 * 1000 // 10 seconds or 10 * 1000 milliseconds
			c.mapperTimerMu.Unlock()
			c.mapperIdMu.Lock()
			c.mapperId[filename.File] = *mapperRep
			c.mapperIdMu.Unlock()
			break
		}
	}
	return nil
}

func (c *Coordinator) MapDone(args bool, done *int) error {
	*done = 1
	c.mapperStateMu.Lock()
	for _, state := range c.mapperState {
		if state != Done {
			*done = 0
			break
		}
	}
	c.mapperStateMu.Unlock()
	if *done == 1 {
		c.mapDoneMu.Lock()
		c.mapdone = true
		c.mapDoneMu.Unlock()
	}
	return nil
}

func (c *Coordinator) UpdateFileMapState(filestate *FileMapStatePair, reply *bool) error {
	filename := filestate.FileName
	curstate := filestate.State
	c.mapperStateMu.Lock()
	defer c.mapperStateMu.Unlock()
	prevstate, ok := c.mapperState[filename]
	if !ok {
		err := fmt.Errorf("%v does not exist", filename)
		return err
	}
	if prevstate != curstate {
		c.mapperState[filename] = curstate
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go c.checkMapTimer()
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.reduceDoneMu.Lock()
	defer c.reduceDoneMu.Unlock()
	return c.reducedone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetOutput(ioutil.Discard)
	c := Coordinator{
		files:        files,
		reducerCount: nReduce,
		mapdone:      false,
		reducedone:   false,
		mapperState:  make(map[string]int),
		mapperTimer:  make(map[string]int),
		mapperId:     make(map[string]WorkerRepStruct),
		reducerState: make(map[int]int),
		reducerTimer: make(map[int]int),
		reducerId:    make(map[int]WorkerRepStruct),
	}
	for _, fileName := range c.files {
		c.mapperState[fileName] = Idle
		c.mapperTimer[fileName] = 0
		c.mapperId[fileName] = WorkerRepStruct{}
	}
	for i := 1; i <= nReduce; i++ {
		c.reducerState[i] = Idle
		c.reducerTimer[i] = 0
		c.reducerId[i] = WorkerRepStruct{}
	}
	c.server()
	return &c
}
