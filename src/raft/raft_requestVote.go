package raft 

import (
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term int // candidate's term!
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term int
	VoteGranted bool

}


func (rf *Raft) election() {

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	supporter := 1 // I voted for myself, and hence:
	rf.lastTouchedAt = time.Now()
	rf.mu.Unlock()

	



    for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue 
		}

		go func(server int) {
			rf.mu.Lock() // ------- 锁 -------
			

			args := &RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,
			}

			reply := &RequestVoteReply{}

			rf.mu.Unlock() // ------- 锁 -------

			ok := rf.sendRequestVote(server, args, reply)

			// server 处理中！

			if !ok {
				return 
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
			}

			if reply.VoteGranted && rf.state == Candidate && rf.currentTerm == args.Term {
				supporter++
				if supporter > len(rf.peers) / 2 {
					// become leader!!!
					rf.state = Leader 
					go rf.leaderTicker()
				}
			}


		}(i)


	}


}




func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	oldTerm := rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	if args.Term < oldTerm {
		reply.VoteGranted = false
		
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId  // 别忘了记录
    	rf.lastTouchedAt = time.Now()   // 重置计时器
	}

	reply.Term = rf.currentTerm



	




}

