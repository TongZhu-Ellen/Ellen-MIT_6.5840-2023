package raft 


type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term int // candidate's term!
	CandidateId int

	// 2B:
	LastLogIndex int 
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term int
	VoteGranted bool

}


func (rf *Raft) collectOpinion() {

	
	var supporter int // I voted for myself
	


    for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			// 不需要打电话的情况！
			rf.mu.Lock()
			if rf.tryVotingFor(rf.me) {
				supporter++
			} else {
				panic ("I can not vote for myself in a new gen!!!")
			}
			rf.mu.Unlock()
			continue
		}

		go func(server int) {
			rf.mu.Lock() // ------- 锁 -------
			lastLogIndex := len(rf.log) - 1
			args := &RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,

				LastLogIndex: lastLogIndex,
				LastLogTerm: rf.log[lastLogIndex].Term,
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
				rf.turnPage(reply.Term)
			}

			if reply.VoteGranted && rf.state == Candidate && rf.currentTerm == args.Term {
				supporter++
				if supporter > len(rf.peers) / 2 {
					rf.state = Leader

					lastLogIndex := len(rf.log) - 1

					rf.nextIndex = make([]int, len(rf.peers)) // "initialized to leader's lastLogIndex + 1"
					rf.matchIndex = make([]int, len(rf.peers)) // "initialized to 0"

					for i := range rf.peers {
						rf.nextIndex[i] = lastLogIndex + 1 
						rf.matchIndex[i] = 0
					}

					rf.matchIndex[rf.me] = lastLogIndex

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

	

	if args.Term > rf.currentTerm {
		rf.turnPage(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return 
		
	} 


	reply.VoteGranted = rf.tryVotingFor(args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	reply.Term = rf.currentTerm



	




}

