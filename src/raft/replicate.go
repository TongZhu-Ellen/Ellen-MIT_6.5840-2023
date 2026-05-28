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
				Entries: append([]Entry{}, rf.log[prevLogIndex+1:]...), // 这里直接引用，因为是本机上，所以直接跑出data-race！
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock() // ----------- 锁 --------------

			ok := rf.sendAppendEntries(server, args, reply) 


			// -------------------------------------- servers处理中！ ---------------------------------------

			if !ok {
				return 
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 更改自身term的逻辑永远先行！
			if reply.Term > rf.currentTerm { 
				rf.newGen(reply.Term)
			}

			// 朝代已然改变！
			if rf.currentTerm != args.Term || rf.state != Leader { 
				return
			}

			// "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry"
			// "If successful: update nextIndex and matchIndex for follower"
			if !reply.Success {
				rf.nextIndex[server]--
				// TODO！！！
			}
            
			newMatch := args.PrevLogIndex + len(args.Entries)
			if newMatch > rf.matchIndex[server] { // TODO: 这里怎么回事？！
				rf.matchIndex[server] = newMatch
				rf.nextIndex[server] = newMatch + 1
				rf.updateCommitIndex()
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

	//  --------------  全局条 --------------
	if args.Term > rf.currentTerm { 
		// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower"
		rf.newGen(args.Term)
	} else if rf.state == Candidate && args.Term == rf.currentTerm {
		// "If AppendEntries RPC received from new leader: convert to follower"
		rf.toFollower()
	}

	

	//  ---------------- 专属条们 -----------------

	 // 1. "Reply false if term < currentTerm"
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}

	// 2. "Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm"
    if args.PrevLogIndex >= len(rf.log) ||
	rf.log[args.PrevLogIndex].Term != args.PrevLogTerm  {
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
		rf.append(args.Entries[yourIdx])
		myIdx++
		yourIdx++
	}

	// 5. "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)

		if rf.commitIndex > rf.lastApplied {
			rf.kick()
		}

	}


	

	
    rf.touched()
	reply.Term = rf.currentTerm
	reply.Success = true

	

} 

