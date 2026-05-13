package raft 

import (
    "time"
)

type AppendEntriesArgs struct {

	Term int // leader's term
	LeaderId int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	
	Term int // my term / follower's term

}

// helper func; can only be called by leader!
func (rf *Raft)  appendYourEntries() {
	
	// 锁内部造好了args（args是只读的，这里确实是只有一份但是没关系！）
	rf.mu.Lock()
	defer rf.mu.Unlock() 

	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	
	// 开goroutine：发送与处理返回！
	for i := 0; i < len(rf.peers); i++ {
		
		go func(server int) {
			// 发送！
		    reply := &AppendEntriesReply{} // reply一人一个！否则你处理的时候就得造新的！并不canonical的行为！
			ok := rf.sendAppendEntries(server, args, reply) 
			



			// 咳咳follower处理中... 

			
			
			// 处理返回！
			if !ok {
				return // follower 死了
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader {
				return // 不是leader了，我们不处理reply了！
			}

			if reply.Term > rf.currentTerm {
				// 退回Follower！
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
			}

			// 不然的话似乎没啥要做的了！


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


	// 上锁！
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// “你还不如我”
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return 
	}

	// 新官当选 所需要做的！
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.lastTouchedAt = time.Now()

	reply.Term = rf.currentTerm
	
} 

