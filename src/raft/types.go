package raft 


import (
    "sync"
    "time"
    "6.5840/labrpc"
)



// ------------- Log Entry -----------

type Entry struct {
    Term    int         // 该 entry 是在哪个 leader term 写入的
    Command interface{} 
}









// --------- ApplyMsg -----------

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











// ----------- Raft ------------





type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	wakeCh    chan struct{}       // 2B我自己加的
	

	// Your data here (2A, 2B, 2C).

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

	// 2D:
	snapIndex int  // 最后一个被 snapshot 的 index
	snapshot []byte

}


