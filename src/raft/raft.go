package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

const (
	HeartBeatTimeout     = 110
	ElectionTimeoutBase  = HeartBeatTimeout * 2
	ElectionTimeoutRange = 150
)

func getRandomElectionTimeout() int {
	return ElectionTimeoutBase + rand.Intn(ElectionTimeoutRange)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type SnapShot struct {
	Snapshot          []byte
	LastIncludedTerm  int
	LastIncludedIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	currentTerm          int
	votedFor             int
	state                int
	electionTimestamp    int64
	heartBeatTimestamp   int64
	electionTimeout      int
	electionTimeoutChan  chan bool // under state of Follower and Candidate
	heartBeatTimeoutChan chan bool // under state of Leader
	stopElectionCond     *sync.Cond
	stopHeartBeatCond    *sync.Cond
	logs                 []ApplyMsg
	commitIndex          int
	lastApplied          int
	nextIndex            []int
	matchIndex           []int
	indexOffset          int
	snapshot             ApplyMsg
	replaySnapshot       bool
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(persistSnapShot bool) {
	stateData := rf.persistState()
	if persistSnapShot {
		rf.persister.SaveStateAndSnapshot(stateData, rf.snapshot.Snapshot)
	} else {
		rf.persister.SaveRaftState(stateData)
	}
}

func (rf *Raft) persistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapshot.SnapshotIndex)
	e.Encode(rf.snapshot.CommandIndex)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(stateData []byte, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(stateData) > 0 {
		rf.readStatePersist(stateData)
	}
	if len(snapshotData) > 0 {
		rf.snapshot.Snapshot = snapshotData
		rf.snapshot.SnapshotValid = true
		rf.replaySnapshot = false
	}
	log.Printf("server %v restart with snapshot index %v, lastapplied %v, snapshot commandindex %v",
		rf.me, rf.snapshot.SnapshotIndex, rf.lastApplied, rf.snapshot.CommandIndex)
}

func (rf *Raft) readStatePersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []ApplyMsg
	var lastIncludedIndex int
	var snapshotCommandIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&snapshotCommandIndex) != nil {
		log.Fatalln("decode error in readStatePersist!")
	} else {
		rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
		rf.snapshot.SnapshotIndex, rf.indexOffset = lastIncludedIndex, lastIncludedIndex
		rf.snapshot.CommandIndex = snapshotCommandIndex
		rf.lastApplied = rf.indexOffset
	}
	for idx, _ := range rf.peers {
		if idx != rf.me {
			rf.nextIndex[idx] = len(rf.logs)
		}
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// An vestigial API interface
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.indexOffset {
		return
	}
	log.Printf("server %v call snapshot with index %v, indexOffset %v and index of last log entry is %v\n", rf.me, index, rf.indexOffset, len(rf.logs)-1)
	rf.snapshot = rf.logs[index-rf.indexOffset]
	rf.snapshot.Snapshot = snapshot
	rf.snapshot.SnapshotIndex = index
	rf.snapshot.SnapshotTerm = rf.logs[index-rf.indexOffset].CommandTerm
	rf.snapshot.SnapshotValid = true
	rf.snapshot.CommandValid = false
	rf.logs = append(rf.logs[0:1], rf.logs[index-rf.indexOffset+1:]...)
	rf.indexOffset = index
	rf.persist(true)
	log.Printf("server %v call snapshot done, indexOffset %v and index of last log entry is %v\n", rf.me, rf.indexOffset, len(rf.logs)-1)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) doVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.votedFor = args.CandidateId
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	rf.persist(false)
	rf.convertTo(Follower)
	rf.resetElectionTimer()
}

func (rf *Raft) isCandLogMoreUpdate(lastLogIndex int, lastLogTerm int) bool {
	if len(rf.logs) == 1 || lastLogTerm > rf.logs[len(rf.logs)-1].CommandTerm {
		return true
	} else if lastLogTerm == rf.logs[len(rf.logs)-1].CommandTerm {
		return lastLogIndex >= rf.logs[len(rf.logs)-1].CommandIndex
	}
	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == args.CandidateId {
			rf.doVote(args, reply)
		} else if rf.votedFor == -1 {
			if rf.isCandLogMoreUpdate(args.LastLogIndex, args.LastLogTerm) {
				rf.doVote(args, reply)
			} else {
				reply.Term, reply.VoteGranted = rf.currentTerm, false
			}
		} else {
			reply.Term, reply.VoteGranted = rf.currentTerm, false
		}
	} else {
		if rf.isCandLogMoreUpdate(args.LastLogIndex, args.LastLogTerm) {
			rf.doVote(args, reply)
		} else {
			reply.Term, reply.VoteGranted = rf.currentTerm, false
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist(false)
			rf.convertTo(Follower)
			rf.resetElectionTimer()
		}
	}
}

type AppendEntryArgs struct {
	Term                  int
	PrevLogIndex          int
	PrevLogTerm           int
	PrevLogIndexForUpdate int
	Entries               []ApplyMsg
	LeaderCommit          int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) installSnapshotAndAppendEntry(args *AppendEntryArgs) int {
	entryCnt := 0
	for _, entry := range args.Entries {
		if entry.SnapshotValid {
			rf.snapshot = entry
			if len(rf.logs)-1+rf.indexOffset >= entry.SnapshotIndex {
				rf.logs = append(rf.logs[0:1], rf.logs[entry.SnapshotIndex+1-rf.indexOffset:]...)
			} else {
				rf.logs = rf.logs[0:1]
			}
			rf.indexOffset = rf.snapshot.SnapshotIndex
			entryCnt++
			log.Printf("server %v install snapshot with index %v\n", rf.me, entry.CommandIndex)
		} else {
			if entry.CommandIndex > len(rf.logs)+rf.indexOffset-1 {
				rf.logs = append(rf.logs, entry)
				entryCnt++
				log.Printf("server %v install command with index %v\n", rf.me, entry.CommandIndex)
			} else if entry.CommandTerm != rf.logs[entry.CommandIndex-rf.indexOffset].CommandTerm {
				rf.logs[entry.CommandIndex-rf.indexOffset] = entry
				entryCnt++
				log.Printf("server %v install command with index %v\n", rf.me, entry.CommandIndex)
			}
		}
	}
	return entryCnt
}

func (rf *Raft) doAppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	reply.Term, reply.Success = args.Term, true
	entryCnt := rf.installSnapshotAndAppendEntry(args)
	log.Printf("server %v do append %v entries\n", rf.me, entryCnt)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	rf.persist(true)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1+rf.indexOffset)
	}
	rf.convertTo(Follower)
	rf.resetElectionTimer()
}

func (rf *Raft) fixLogs(args *AppendEntryArgs, reply *AppendEntryReply) {
	log.Printf("server %v do fix logs with PrevLogIndex %v, PrevLogTerm: %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
	if len(rf.logs)+rf.indexOffset > args.PrevLogIndex {
		log.Printf("server %v conflicts with log term %v\n", rf.me, rf.logs[args.PrevLogIndex-rf.indexOffset].CommandTerm)
	} else {
		log.Printf("server %v index of last log entry is %v\n", rf.me, len(rf.logs)-1+rf.indexOffset)
	}
	reply.Term, reply.Success = args.Term, false
	if len(rf.logs)+rf.indexOffset > args.PrevLogIndex {
		reply.XTerm = rf.logs[args.PrevLogIndex-rf.indexOffset].CommandTerm
		xindex := args.PrevLogIndex
		for xindex > rf.indexOffset && rf.logs[xindex-rf.indexOffset-1].CommandTerm == reply.XTerm {
			xindex--
		}
		reply.XIndex = xindex
		rf.logs = rf.logs[:xindex-rf.indexOffset]
		rf.persist(false)
	} else {
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - (len(rf.logs) + rf.indexOffset) + 1
	}
	log.Printf("server %v reply with xterm: %v xindex: %v xlen: %v\n",
		rf.me, reply.XTerm, reply.XIndex, reply.XLen)
	rf.convertTo(Follower)
	rf.resetElectionTimer()
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
	} else if args.PrevLogIndex != 0 && args.PrevLogIndex < rf.indexOffset {
		reply.Term, reply.Success = args.Term, true
	} else {
		if args.PrevLogIndex == 0 || args.PrevLogIndex == rf.indexOffset {
			rf.doAppendEntry(args, reply)
		} else if len(rf.logs)+rf.indexOffset > args.PrevLogIndex &&
			rf.logs[args.PrevLogIndex-rf.indexOffset].CommandTerm == args.PrevLogTerm {
			rf.doAppendEntry(args, reply)
		} else {
			rf.fixLogs(args, reply)
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term, isLeader = len(rf.logs), rf.currentTerm, rf.state == Leader
	if isLeader {
		entry := ApplyMsg{
			CommandValid: false,
			Command:      command,
			CommandIndex: index + rf.indexOffset,
			CommandTerm:  term,
		}
		log.Printf("server %v add an entry with commandIndex %v\n", rf.me, entry.CommandIndex)
		rf.logs = append(rf.logs, entry)
		rf.persist(false)
		go rf.broadcastHeartbeat(false, index, rf.currentTerm)
	}
	return index + rf.indexOffset, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTimeTicker() {
	for !rf.killed() {
		if _, isleader := rf.GetState(); isleader {
			rf.mu.Lock()
			rf.stopElectionCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			currentTime := time.Now().UnixMilli()
			if currentTime-rf.electionTimestamp >= int64(rf.electionTimeout) {
				rf.mu.Unlock()
				rf.electionTimeoutChan <- true
			} else {
				rf.mu.Unlock()
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimestamp = time.Now().UnixMilli()
	rf.electionTimeout = getRandomElectionTimeout()
}

func (rf *Raft) heartBeatTimeTicker() {
	for !rf.killed() {
		if _, isleader := rf.GetState(); !isleader {
			rf.mu.Lock()
			rf.stopHeartBeatCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			currentTime := time.Now().UnixMilli()
			if currentTime-rf.heartBeatTimestamp >= int64(HeartBeatTimeout) {
				rf.mu.Unlock()
				rf.heartBeatTimeoutChan <- true
			} else {
				rf.mu.Unlock()
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.heartBeatTimestamp = time.Now().UnixMilli()
}

func (rf *Raft) convertTo(state int) {
	stateMap := map[int]string{
		0: "Follower",
		1: "Candidate",
		2: "Leader",
	}
	log.Printf("server %v convert to %v\n", rf.me, stateMap[state])
	switch state {
	case Follower:
		rf.state = Follower
		rf.stopElectionCond.Signal()
	case Candidate:
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.persist(false)
		rf.stopElectionCond.Signal()
	case Leader:
		rf.state = Leader
		rf.stopHeartBeatCond.Signal()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.convertTo(Candidate)
	log.Printf("server %v start election with currentTerm %v\n", rf.me, rf.currentTerm)
	reqVoteArg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1 + rf.indexOffset,
		LastLogTerm:  rf.logs[len(rf.logs)-1].CommandTerm,
	}
	rf.mu.Unlock()

	var wg sync.WaitGroup
	votedCnt := int32(1)
	for idx, _ := range rf.peers {
		if idx != rf.me {
			wg.Add(1)
			go func(peerId int, reqVoteArg RequestVoteArgs) {
				defer wg.Done()
				reqVoteReply := RequestVoteReply{}
				ok := rf.sendRequestVote(peerId, &reqVoteArg, &reqVoteReply)
				if ok {
					term, granted := reqVoteReply.Term, reqVoteReply.VoteGranted
					if granted {
						log.Printf("server %v received vote from server %v\n", rf.me, peerId)
						atomic.AddInt32(&votedCnt, 1)
						if atomic.LoadInt32(&votedCnt) == int32(len(rf.peers)/2+1) {
							rf.mu.Lock()
							rf.convertTo(Leader)
							for idx, _ := range rf.peers {
								if idx != rf.me {
									rf.nextIndex[idx] = len(rf.logs)
									rf.matchIndex[idx] = 0
								}
							}
							go rf.broadcastHeartbeat(true, -1, -1)
							rf.mu.Unlock()
						}
					} else {
						func(term int) {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if term > rf.currentTerm {
								rf.currentTerm = term
								rf.persist(false)
								rf.convertTo(Follower)
								rf.resetElectionTimer()
							}
						}(term)
					}
				}
			}(idx, reqVoteArg)
		}
	}
	wg.Wait()
}

func (rf *Raft) commitEntry() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 10)
		tmpEntryList := []ApplyMsg{}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.snapshot.SnapshotIndex > rf.lastApplied || (rf.snapshot.SnapshotValid && !rf.replaySnapshot) {
				tmpEntryList = append(tmpEntryList, rf.snapshot)
				if !rf.replaySnapshot {
					log.Printf("server %v replay snapshot with commandindex %v\n", rf.me, rf.snapshot.CommandIndex)
				}
				rf.replaySnapshot = true
				rf.lastApplied = max(rf.snapshot.SnapshotIndex, rf.lastApplied)
			}
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				tmpEntryList = append(tmpEntryList, rf.logs[rf.lastApplied-rf.indexOffset])
			}
		}()
		for _, entry := range tmpEntryList {
			if !entry.SnapshotValid {
				log.Printf("server %v commits entry %v... (command)\n", rf.me, entry.CommandIndex)
				entry.CommandValid = true
			} else {
				log.Printf("server %v commits entry %v... (snapshot)\n", rf.me, entry.CommandIndex)
				entry.CommandValid = false
			}
			rf.applyCh <- entry
		}
	}
}

func (rf *Raft) dealSucessBroadcast(peerId int,
	appendArgs *AppendEntryArgs,
	appendReply *AppendEntryReply,
	appendCnt *map[int]int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("server %v send append entry to server %v success\n", rf.me, peerId)
	if rf.state != Leader {
		log.Printf("server %v has not been Leader\n", rf.me)
		return
	}
	if len(appendArgs.Entries) == 0 {
		rf.matchIndex[peerId] = max(appendArgs.PrevLogIndexForUpdate, rf.matchIndex[peerId])
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
	} else {
		rf.matchIndex[peerId] = max(rf.matchIndex[peerId], appendArgs.Entries[len(appendArgs.Entries)-1].CommandIndex)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
	}
	if rf.matchIndex[peerId] > rf.commitIndex && rf.matchIndex[peerId] < len(rf.logs)+rf.indexOffset {
		entry := &rf.logs[rf.matchIndex[peerId]-rf.snapshot.SnapshotIndex]
		if entry.CommandTerm == rf.currentTerm {
			_, ok := (*appendCnt)[entry.CommandIndex]
			if ok {
				(*appendCnt)[entry.CommandIndex]++
			} else {
				(*appendCnt)[entry.CommandIndex] = 2
			}
			if (*appendCnt)[entry.CommandIndex] >= len(rf.peers)/2+1 {
				rf.commitIndex = max(rf.commitIndex, entry.CommandIndex)
			}
		}
	}
}

func (rf *Raft) fixAppendArgsLogs(appendArgs *AppendEntryArgs, st int, ed int) {
	if appendArgs.PrevLogIndex <= rf.snapshot.SnapshotIndex {
		startIndex := min(rf.snapshot.SnapshotIndex-appendArgs.PrevLogIndex, len(appendArgs.Entries))
		appendArgs.Entries = appendArgs.Entries[startIndex:]
		appendArgs.Entries = append([]ApplyMsg{rf.snapshot}, appendArgs.Entries...)
		appendArgs.PrevLogIndex = 0
	} else {
		for i := ed; i >= st; i-- {
			if i <= rf.snapshot.SnapshotIndex {
				msg := rf.snapshot
				msg.SnapshotValid = true
				appendArgs.Entries = append([]ApplyMsg{msg}, appendArgs.Entries...)
				appendArgs.PrevLogIndex = 0
				break
			} else {
				msg := rf.logs[i-rf.snapshot.SnapshotIndex]
				appendArgs.Entries = append([]ApplyMsg{msg}, appendArgs.Entries...)
			}
		}
	}
}

func (rf *Raft) dealFailedBroadcast(peerId int,
	appendArgs *AppendEntryArgs,
	appendReply *AppendEntryReply,
	appendCnt *map[int]int) {
	rf.mu.Lock()
	log.Printf("server %v send append entry to server %v failed\n", rf.me, peerId)
	if rf.state != Leader {
		log.Printf("server %v has not been Leader now\n", rf.me)
		rf.mu.Unlock()
	} else if appendReply.Term > appendArgs.Term {
		rf.currentTerm = max(appendReply.Term, rf.currentTerm)
		rf.persist(false)
		rf.convertTo(Follower)
		rf.resetElectionTimer()
		rf.mu.Unlock()
	} else {
		if appendReply.XTerm != -1 {
			rf.fixAppendArgsLogs(appendArgs, appendReply.XIndex, appendArgs.PrevLogIndex)
			if appendArgs.PrevLogIndex != 0 {
				appendArgs.PrevLogIndex = appendReply.XIndex - 1
			}
		} else {
			rf.fixAppendArgsLogs(appendArgs, appendArgs.PrevLogIndex-appendReply.XLen+1, appendArgs.PrevLogIndex)
			if appendArgs.PrevLogIndex != 0 {
				appendArgs.PrevLogIndex -= appendReply.XLen
			}
		}
		if appendArgs.PrevLogIndex != 0 {
			appendArgs.PrevLogTerm = rf.logs[appendArgs.PrevLogIndex-rf.snapshot.SnapshotIndex].CommandTerm
		} else {
			appendArgs.PrevLogTerm = 0
		}
		rf.nextIndex[peerId] = appendArgs.PrevLogIndex + 1
		rf.mu.Unlock()
		rf.trySendAppendEntries(peerId, appendArgs, appendReply, appendCnt)
	}
}

func (rf *Raft) trySendAppendEntries(peerId int,
	appendArgs *AppendEntryArgs,
	appendReply *AppendEntryReply,
	appendCnt *map[int]int) {
	*appendReply = AppendEntryReply{}
	ok := rf.sendAppendEntries(peerId, appendArgs, appendReply)
	if !ok {
		return
	} else {
		if appendReply.Success {
			rf.dealSucessBroadcast(peerId, appendArgs, appendReply, appendCnt)
		} else {
			rf.dealFailedBroadcast(peerId, appendArgs, appendReply, appendCnt)
		}
	}
}

func (rf *Raft) prepareAppendArgs(isHeartBeat bool, index int, term int) (AppendEntryArgs, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return AppendEntryArgs{}, false
	}
	if !isHeartBeat && (index != len(rf.logs)-1 || term != rf.currentTerm) {
		return AppendEntryArgs{}, false
	}
	rf.resetHeartBeatTimer()
	appendArgs := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex,
	}
	if isHeartBeat {
		appendArgs.PrevLogIndex = len(rf.logs) - 1 + rf.indexOffset
		appendArgs.PrevLogIndexForUpdate = appendArgs.PrevLogIndex
		appendArgs.PrevLogTerm = rf.logs[len(rf.logs)-1].CommandTerm
	} else {
		appendArgs.PrevLogIndex = len(rf.logs) - 2 + rf.indexOffset
		appendArgs.PrevLogTerm = rf.logs[len(rf.logs)-2].CommandTerm
		appendArgs.Entries = append(appendArgs.Entries, rf.logs[len(rf.logs)-1])
	}
	if isHeartBeat {
		log.Printf("server %v broadcast heartbeat for heartbeat or election triumph| currentTerm: %v prevLogIndex: %v\n", rf.me, rf.currentTerm, appendArgs.PrevLogIndex)
	} else {
		log.Printf("server %v broadcast heartbeat for new command | currentTerm: %v prevLogIndex: %v\n", rf.me, rf.currentTerm, appendArgs.PrevLogIndex)
	}
	return appendArgs, true
}

func (rf *Raft) broadcastHeartbeat(isHeartBeat bool, index int, term int) {
	appendArgs, ok := rf.prepareAppendArgs(isHeartBeat, index, term)
	if ok {
		var wg sync.WaitGroup
		appendCnt := make(map[int]int)
		for idx, _ := range rf.peers {
			if idx != rf.me {
				wg.Add(1)
				go func(peerId int, appendArgs AppendEntryArgs) {
					defer wg.Done()
					appendReply := AppendEntryReply{}
					rf.trySendAppendEntries(peerId, &appendArgs, &appendReply, &appendCnt)
				}(idx, appendArgs)
			}
		}
		wg.Wait()
	}
}

func (rf *Raft) ticker() {
	go rf.electionTimeTicker()
	go rf.heartBeatTimeTicker()
	go rf.commitEntry()
	for !rf.killed() {
		select {
		case <-rf.electionTimeoutChan:
			go rf.startElection()
		case <-rf.heartBeatTimeoutChan:
			go rf.broadcastHeartbeat(true, -1, -1)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetOutput(io.Discard)
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = Follower
	rf.votedFor = -1
	rf.resetElectionTimer()
	rf.resetHeartBeatTimer()
	rf.electionTimeoutChan = make(chan bool)
	rf.heartBeatTimeoutChan = make(chan bool)
	rf.stopElectionCond = sync.NewCond(&rf.mu)
	rf.stopHeartBeatCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.indexOffset = 0
	rf.snapshot = ApplyMsg{}
	rf.replaySnapshot = true
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.logs = append(rf.logs, ApplyMsg{
		CommandValid: true,
		CommandIndex: 0,
		CommandTerm:  0,
	})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
