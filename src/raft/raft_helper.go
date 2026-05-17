package raft 

/*
持锁的一个子行为。
在论文里面对应的是 If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower 

这个函数本身没有锁！只能锁内调用！
*/

func (rf *Raft) toFollower(term int) { 
	

	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	
}