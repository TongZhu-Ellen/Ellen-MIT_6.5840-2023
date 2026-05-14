package raft 

import (
    "time"
)

type AppendEntriesArgs struct {

	Term int // leader's term
	LeaderId int

	// 2B:
	PrevLogTerm int 

	PrevLogIndex int
	Entries []Entry // 新的一串儿，全部新的。

}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	
	Term int // my term / follower's term
	Success bool

}

// helper func; can only be called by leader!
func (rf *Raft)  appendYourEntries() {
	
	
	
	
	// 开goroutine：发送与处理返回！
	for i := 0; i < len(rf.peers) && i != rf.me; i++ {

		 if i == rf.me {
			continue
		}
		
		
		go func(server int) {
			rf.mu.Lock() // -------- 锁区 --------

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
				Entries:      rf.log[rf.nextIndex[server]:],
			}

		    reply := &AppendEntriesReply{}


			rf.mu.Unlock() // -------- 锁区 --------


			// 发送！
			ok := rf.sendAppendEntries(server, args, reply) 
			



			// 咳咳follower处理中... 

			
			
			// 处理返回！
			if !ok {
				return // follower 死了
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()


			if reply.Term > rf.currentTerm {
				// 退回Follower！
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				return
			}

			// 2B:
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1 // 对方现在确认到的最大索引+1
				// TODO：2B 关于commit的一切
			} else {
				nextIndex[server]--
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


	// 上锁！
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// “你还不如我”
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}

	// 新官当选 所需要做的！
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// -------------------下面是2B处理逻辑-------------------


	// if: log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm 
    // reply false!
	if len(rf.log) - 1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}

	// 从 PrevLogIndex+1 开始截断并粘贴新 entries
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)



	// -----------------2B 结束 ----------------------------




	rf.lastTouchedAt = time.Now()

	reply.Term = rf.currentTerm
	reply.Success = true
	
} 

