package raft 

import "time"



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
	XTerm   int  // 冲突的 term
    XIndex  int  // 该 term 第一条 log 的 index
    XLen    int  // log 长度（用于 prevLogIndex 超出范围的情况）


}

// helper func; can only be called by leader!
func (rf *Raft) replicator(i int) {


	for !rf.killed() {

		retry := true

		for retry {
		    retry = rf.singleAppend(i)
		}

		time.Sleep(HEATBEAT_INTERVAL)
	}
	

}

func (rf *Raft) singleAppend(i int) (retry bool) {
	
	rf.mu.Lock() // ----------- 锁 --------------
	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}
	prevLogIndex := rf.nextIndex[i] - 1

	// 2D:
	if prevLogIndex < rf.snapIndex {
		rf.helpInstall(i) 
		rf.mu.Unlock()
		return false
	}
	
    args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: rf.get(prevLogIndex).Term,
		Entries: rf.entriesFrom(prevLogIndex+1), 
		LeaderCommit: rf.commitIndex,
	}
    reply := &AppendEntriesReply{}
    rf.mu.Unlock() // ----------- 锁 --------------

    ok := rf.sendAppendEntries(i, args, reply) 

    // ----------- Server 处理中！ --------------

	if !ok { // 这是没发出去...  
		time.Sleep(10 * time.Millisecond)
		return true
	}


	rf.mu.Lock() // ----------- 锁 --------------
    defer rf.mu.Unlock()

    // 更改自身term的逻辑永远先行！
    if reply.Term > rf.currentTerm { 
		rf.newGen(reply.Term)
		return false
	}

	// 朝代已然改变！
    if rf.currentTerm != args.Term || rf.state != Leader {
		return false
	}


	// "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry"
    // "If successful: update nextIndex and matchIndex for follower"
    if !reply.Success {
		rf.stepBack(i, reply.XTerm, reply.XIndex, reply.XLen)
		return true
	}


	rf.matchIndex[i] = prevLogIndex + len(args.Entries)
	rf.nextIndex[i]  = rf.matchIndex[i] + 1
	rf.updateCommitIndex()
	return false
}

 

func (rf *Raft) sendAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
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

	rf.touched()

	

	// 2D:
	if args.PrevLogIndex <= rf.snapIndex {  // leader发来的prevLogIndex在快照范围内，一致性自动满足
		reply.Term = rf.currentTerm
		reply.Success = true
		
		appendStart := rf.snapIndex - args.PrevLogIndex  // 算出entries中第一个不在快照内的位置
		if appendStart >= len(args.Entries) {            // entries全部在快照内，无需处理
			rf.tryUpdateCommit(args.LeaderCommit)
			return
		}
		
		rf.reconcileEntries(rf.snapIndex + 1, appendStart, args.Entries)  // 从快照后第一条开始对账
		rf.tryUpdateCommit(args.LeaderCommit)
		return
	}
	


	// 2. "Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm"
    if args.PrevLogIndex >= rf.logLength() {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.logLength()
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if rf.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.XTerm = rf.get(args.PrevLogIndex).Term
		xIndex := args.PrevLogIndex
		for xIndex-1 >= rf.snapIndex+1 && rf.get(xIndex-1).Term == reply.XTerm {
			xIndex--
		}
		reply.XIndex = xIndex
		reply.XLen = -1
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}



	// 3. "If an existing entry conflicts with a new one (same index but different terms), 
	// delete the existing entry and all that follow it"
	// 4. "Append any new entries not already in the log"

	rf.reconcileEntries(args.PrevLogIndex + 1, 0, args.Entries)
	

	
	

	// 5. "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
	rf.tryUpdateCommit(args.LeaderCommit)


	

	
   

	reply.Term = rf.currentTerm
	reply.Success = true

	

} 
