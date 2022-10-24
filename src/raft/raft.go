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

	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Index   int
	Message ApplyMsg
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	candTerm, candId := args.Term, args.CandidateId
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if candTerm < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
	} else if candTerm == rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == candId {
			reply.Term, reply.VoteGranted = rf.currentTerm, true
			rf.votedFor = candId
			rf.convertTo(Follower)
			rf.resetElectionTimer()
		} else {
			reply.Term, reply.VoteGranted = rf.currentTerm, false
		}
	} else {
		reply.Term, reply.VoteGranted = candTerm, true
		rf.currentTerm = candTerm
		rf.votedFor = candId
		rf.convertTo(Follower)
		rf.resetElectionTimer()
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
	} else {
		reply.Term, reply.Success = args.Term, true
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		rf.resetElectionTimer()
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

	return index, term, isLeader
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
			currentTime := time.Now().UnixMilli()
			rf.mu.Lock()
			if currentTime-rf.electionTimestamp >= int64(rf.electionTimeout) {
				rf.mu.Unlock()
				rf.electionTimeoutChan <- true
			} else {
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * 10)
			}
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
			currentTime := time.Now().UnixMilli()
			rf.mu.Lock()
			if currentTime-rf.heartBeatTimestamp >= HeartBeatTimeout {
				rf.mu.Unlock()
				rf.heartBeatTimeoutChan <- true
			} else {
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.heartBeatTimestamp = time.Now().UnixMilli()
}

func (rf *Raft) convertTo(state int) {
	switch state {
	case Follower:
		rf.state = Follower
		rf.stopElectionCond.Signal()
	case Candidate:
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.currentTerm++
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
		Term:        rf.currentTerm,
		CandidateId: rf.me,
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
				if !ok {
					log.Printf("server %v send vote request to server %v failed\n", rf.me, peerId)
				} else {
					term, granted := reqVoteReply.Term, reqVoteReply.VoteGranted
					if granted {
						log.Printf("server %v received vote from server %v\n", rf.me, peerId)
						atomic.AddInt32(&votedCnt, 1)
						if atomic.LoadInt32(&votedCnt) == int32(len(rf.peers)/2+1) {
							rf.mu.Lock()
							rf.convertTo(Leader)
							rf.mu.Unlock()
							go rf.broadcastHeartbeat()
						}
					} else {
						func(term int) {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if term > rf.currentTerm {
								rf.currentTerm = term
								rf.convertTo(Follower)
							}
						}(term)
					}
				}
			}(idx, reqVoteArg)
		}
	}
	wg.Wait()
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	log.Printf("server %v start broadcast with currentTerm %v\n", rf.me, rf.currentTerm)
	rf.resetHeartBeatTimer()
	appendArgs := AppendEntryArgs{
		Term: rf.currentTerm,
	}
	rf.mu.Unlock()

	var wg sync.WaitGroup
	for idx, _ := range rf.peers {
		if idx != rf.me {
			wg.Add(1)
			go func(peerId int, appendArgs AppendEntryArgs) {
				defer wg.Done()
				appendReply := AppendEntryReply{}
				ok := rf.sendAppendEntries(peerId, &appendArgs, &appendReply)
				if !ok {
					log.Printf("server %v send appendentry to server %v failed\n", rf.me, peerId)
				} else {
					term, success := appendReply.Term, appendReply.Success
					func(term int, success bool) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if !success && term > rf.currentTerm {
							rf.currentTerm = term
							rf.convertTo(Follower)
						}
					}(term, success)
				}
			}(idx, appendArgs)
		}
	}
	wg.Wait()
}

func (rf *Raft) ticker() {
	go rf.electionTimeTicker()
	go rf.heartBeatTimeTicker()
	for !rf.killed() {
		select {
		case <-rf.electionTimeoutChan:
			go rf.startElection()
		case <-rf.heartBeatTimeoutChan:
			go rf.broadcastHeartbeat()
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
