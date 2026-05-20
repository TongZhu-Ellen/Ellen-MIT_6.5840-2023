package raft 



type AppendEntriesArgs struct {

	Term int // leader's term
	LeaderId int

	// 2B:
	PrevLogIndex int // 上次的最后一条，
	PrevLogTerm int 
	Entries []Entry
	LeaderCommit int

}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	
	Term int // my term / follower's term
	
	// 2B:
	Success bool

}

// helper func; can only be called by leader!
func (rf *Raft)  appendYourEntries() {
	
	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		go func(server int) {

			
			// 2B:
			// "If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex"
			rf.mu.Lock() // ----------- 锁 --------------
			prevLogIndex := rf.nextIndex[server] - 1
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: rf.log[prevLogIndex].Term,
				Entries: rf.log[prevLogIndex+1 : ],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock() // ----------- 锁 --------------

			ok := rf.sendAppendEntries(server, args, reply) 


			// servers处理中！

			if !ok {
				return 
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.turnPage(reply.Term)
			}

			if term, leads := rf.GetState(); term != args.Term || !leads {
				return
			}

			// "If successful: update nextIndex and matchIndex for follower"
			// "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry "
			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else {
				rf.nextIndex[server]--
			}

			
		    // If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
			for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
				count := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] >= N {
						count++
					}
				}
				if count > len(rf.peers) / 2 && rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
					break
				}
			}



		}(i)

	}


}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example AppendEntries RPC handler.
// 这是follower方的处理函数！
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).


	rf.mu.Lock()
	defer rf.mu.Unlock()

	//  --------------  全局条2 --------------
	if args.Term > rf.currentTerm { 
		// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower"
		rf.turnPage(args.Term)
	} else if rf.state == Candidate && args.Term == rf.currentTerm {
		// "If AppendEntries RPC received from new leader: convert to follower"
		rf.ripPage()
	}

	

	//  ---------------- 专属条们 -----------------

	 // 1. "Reply false if term < currentTerm"
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}

	// 2. "Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm"
    if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}


	// 3. "If an existing entry conflicts with a new one (same index but different terms), 
	// delete the existing entry and all that follow it"
	myIdx := args.PrevLogIndex + 1
	yourIdx := 0 

	for myIdx < len(rf.log) && yourIdx < len(args.Entries) {
		if rf.log[myIdx].Term != args.Entries[yourIdx].Term {
			rf.log = rf.log[ : myIdx] // 连同这个也不要了。
			break
		} 
		myIdx++
		yourIdx++
	}

	// 4. "Append any new entries not already in the log"
	for yourIdx < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[yourIdx])
		myIdx++
		yourIdx++
	}

	// 5. "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	}


	



	

    rf.touched()
	reply.Term = rf.currentTerm
	reply.Success = true

	

} 

