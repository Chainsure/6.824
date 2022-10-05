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
	mapCondMu     sync.Mutex
	reduceCondMu  sync.Mutex
	mapCond       *sync.Cond
	reduceCond    *sync.Cond
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
		allDone := true
		time.Sleep(time.Millisecond * 100)
		c.mapperStateMu.Lock()
		for file, state := range c.mapperState {
			allDone = allDone && (state == Done)
			if state == Processing {
				c.mapperTimerMu.Lock()
				c.mapperTimer[file] -= 100
				if c.mapperTimer[file] <= 0 {
					c.mapperState[file] = Idle
					c.mapperIdMu.Lock()
					c.mapperId[file] = WorkerRepStruct{}
					c.mapperIdMu.Unlock()
					c.mapCond.Signal()
				}
				c.mapperTimerMu.Unlock()
			}
		}
		c.mapperStateMu.Unlock()
		if allDone {
			break
		}
	}
	for {
		allDone := true
		time.Sleep(time.Millisecond * 100)
		c.reduceStateMu.Lock()
		for id, state := range c.reducerState {
			allDone = allDone && (state == Done)
			if state == Processing {
				c.reduceTimerMu.Lock()
				c.reducerTimer[id] -= 100
				if c.reducerTimer[id] <= 0 {
					c.reducerState[id] = Idle
					c.reduceIdMu.Lock()
					c.reducerId[id] = WorkerRepStruct{}
					c.reduceIdMu.Unlock()
					c.reduceCond.Signal()
				}
				c.reduceTimerMu.Unlock()
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
	defer c.reduceStateMu.Unlock()
	prevState, ok := c.reducerState[reduceIdx]
	if !ok {
		err := fmt.Errorf("%v does not exist in reduceStateMap", reduceIdx)
		return err
	}
	if prevState != Done && prevState != state {
		c.reducerState[reduceIdx] = state
		if state == Idle {
			c.reduceCond.Signal()
		}
	}
	return nil
}

func (c *Coordinator) GetReduceIdx(reducerRep *WorkerRepStruct, reduceIdx *int) error {
	*reduceIdx = -1
	allDone := true
	c.reduceStateMu.Lock()
	for i := 1; i <= c.reducerCount; i++ {
		allDone = allDone && (c.reducerState[i] == Done)
		if c.reducerState[i] == Idle {
			c.reduceTimerMu.Lock()
			c.reducerTimer[i] = 10 * 1000
			c.reduceTimerMu.Unlock()
			c.reduceIdMu.Lock()
			c.reducerId[i] = *reducerRep
			c.reduceIdMu.Unlock()
			c.reducerState[i] = Processing
			*reduceIdx = i
			break
		}
	}
	c.reduceStateMu.Unlock()
	if *reduceIdx == -1 && !allDone {
		c.reduceCond.L.Lock()
		c.reduceCond.Wait()
		c.reduceCond.L.Unlock()
	}
	return nil
}

func (c *Coordinator) ReduceDone(args bool, done *bool) error {
	*done = true
	c.reduceStateMu.Lock()
	for _, reduceState := range c.reducerState {
		if reduceState != Done {
			*done = false
			break
		}
	}
	c.reduceStateMu.Unlock()
	if *done {
		c.reduceDoneMu.Lock()
		c.reducedone = true
		c.reduceDoneMu.Unlock()
		c.reduceCond.Broadcast()
	}
	return nil
}

func (c *Coordinator) GetFileMapper(filename string, mapperRep *WorkerRepStruct) error {
	c.mapperIdMu.Lock()
	rep, ok := c.mapperId[filename]
	c.mapperIdMu.Unlock()
	if !ok {
		log.Printf("cannot find file %v in mapperId\n", filename)
	} else {
		*mapperRep = rep
	}
	return nil
}

func (c *Coordinator) GetReducerCnt(arg bool, reducerCnt *int) error {
	*reducerCnt = c.reducerCount
	return nil
}

func (c *Coordinator) GetMapFile(mapperRep *WorkerRepStruct, filename *string) error {
	c.mapperStateMu.Lock()
	allDone := true
	for file, state := range c.mapperState {
		allDone = allDone && (state == Done)
		if state == Idle {
			*filename = file
			c.mapperState[file] = Processing
			c.mapperTimerMu.Lock()
			c.mapperTimer[*filename] = 10 * 1000 // 10 seconds or 10 * 1000 milliseconds
			c.mapperTimerMu.Unlock()
			c.mapperIdMu.Lock()
			c.mapperId[*filename] = *mapperRep
			c.mapperIdMu.Unlock()
			break
		}
	}
	c.mapperStateMu.Unlock()
	if *filename == "" && !allDone {
		c.mapCond.L.Lock()
		c.mapCond.Wait()
		c.mapCond.L.Unlock()
	}
	return nil
}

func (c *Coordinator) MapDone(args bool, done *bool) error {
	*done = true
	c.mapperStateMu.Lock()
	for _, state := range c.mapperState {
		if state != Done {
			*done = false
			break
		}
	}
	c.mapperStateMu.Unlock()
	if *done {
		c.mapDoneMu.Lock()
		c.mapdone = true
		c.mapDoneMu.Unlock()
		c.mapCond.Broadcast()
	}
	return nil
}

func (c *Coordinator) UpdateFileMapState(filestate *FileMapStatePair, reply *bool) error {
	filename, curstate := filestate.FileName, filestate.State
	c.mapperStateMu.Lock()
	defer c.mapperStateMu.Unlock()
	prevstate, ok := c.mapperState[filename]
	if !ok {
		err := fmt.Errorf("%v does not exist", filename)
		return err
	}
	if prevstate != Done && prevstate != curstate {
		c.mapperState[filename] = curstate
		if curstate == Idle {
			c.mapCond.Signal()
		}
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
		mapCondMu:    sync.Mutex{},
		reduceCondMu: sync.Mutex{},
	}
	c.mapCond = sync.NewCond(&c.mapCondMu)
	c.reduceCond = sync.NewCond(&c.reduceCondMu)
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
