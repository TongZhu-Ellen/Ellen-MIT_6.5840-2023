package raft 

import "time"

/*
持锁的一个子行为。
在论文里面对应的是 If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower 

这个函数本身没有锁！只能锁内调用！
*/



func (rf *Raft) turnPage(term int) { 

	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1

}

func (rf *Raft) ripPage() {

	rf.state = Follower
	rf.votedFor = -1

}



func (rf *Raft) tryVotingFor(candidate int, lastLogIndex int, lastLogTerm int) bool {
	
	myLastTerm := rf.log[len(rf.log)-1].Term
	myLastIndex := len(rf.log) - 1
	
	upToDate := lastLogTerm > myLastTerm || 
            (lastLogTerm == myLastTerm && lastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == candidate) && upToDate {
		rf.votedFor = candidate
		rf.lastTouchedAt = time.Now()
		return true
	}

	return false
}

func (rf *Raft) touched() {

	rf.lastTouchedAt = time.Now()
}

func min(a int, b int) int {
	if a <= b {
		return a
	} 
	return b
}




