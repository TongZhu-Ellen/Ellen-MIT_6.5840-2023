package raft

import (
	"sync"
	"sync/atomic"
	"math/rand"
	"time"
	// "6.5840/labgob"
	"6.5840/labrpc"
)

const (
	SELECTION_TIMEOUT = 900 * time.Millisecond
	HEATBEAT_INTERVAL = 100 * time.Millisecond
)



type Entry struct {
	Term int 
	Command interface{}	
}


type RaftState int
const (
    Follower  RaftState = iota // 0
    Candidate                   // 1
    Leader                      // 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 2A: 
	currentTerm int = 0
	state RaftState = Follower
	votedFor int = -1
	
	lastTouchedAt time.Time = time.Now() // 一上来就触发选举很显然是不对的。

	// 2B:
	log []Entry = make([]Entry, 1)
	nextIndex []int // 只有leader会需要的东西。nextIdx[server]




}

func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		if rf.state != Leader && time.Since(rf.lastTouchedAt) > SELECTION_TIMEOUT {
			rf.state = Candidate
			go rf.election()
		}

		rf.mu.Unlock()


		
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		

	}
}

func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		rf.appendYourEntries()

	    time.Sleep(HEATBEAT_INTERVAL)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	
	


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
