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
    Term    int         // 该 entry 是在哪个 leader term 写入的
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	state RaftState
	votedFor int 
	
	lastTouchedAt time.Time
	log []Entry

	commitIndex int 
	lastApplied int

	// for leader only:
	nextIndex []int
	matchIndex []int

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	rf.lastTouchedAt = time.Now() // 一上来就触发选举很显然是不对的。
	rf.log = make([]Entry, 1) // first index is 1 


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) GetState() (int, bool) {

	if rf.killed() {
		return -1, false
	}

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

func (rf *Raft) leaderTicker() {
    for {
    
        if _, leads := rf.GetState(); !leads {
			return 
		}
        
        rf.appendYourEntries()
        time.Sleep(HEATBEAT_INTERVAL)
    }
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock() // ------- 锁! -------
		if time.Since(rf.lastTouchedAt) > SELECTION_TIMEOUT && rf.state != Leader {

			
			rf.turnPage(rf.currentTerm + 1)
			rf.state = Candidate
			go rf.collectOpinion()
		}
		rf.mu.Unlock() // ------- 锁! -------


	
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		

	}
}




