package raft


// 这些全部线程不安全的！
// I do not lock in helpers! as in 2A!


func (rf *Raft) becomeLeader() {
	rf.state = Leader

	lastLogIndex := len(rf.log) - 1

	rf.nextIndex = make([]int, len(rf.peers))  // "initialized to leader's lastLogIndex + 1"
	rf.matchIndex = make([]int, len(rf.peers)) // "initialized to 0"

	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	rf.nextIndex[rf.me] = -1
	rf.matchIndex[rf.me] = -1
}


/*

	If there exists an N such that N > commitIndex, a majority
	of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N



					  idx=1  idx=2  idx=3
					 <<<---------------
					┌──────┬──────┬──────┐
			 A      │      │  ✓   │  ✓   │
			 B      │      │  ✓   │      │
			 C*     │  ✓   │  ✓   │  ✓   │
			 D      │      │      │      │
					├──────┼──────┼──────┤
			 count  │      │  3   │  2   │
			    	└──────┴──────┴──────┘
							✓bingo!




*/
// leader专属函数
func (rf *Raft) updateCommitIndex() {
    // 从最新日志往前找，寻找可以提交的最大 N
    for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
        count := 0
        for i := 0; i < len(rf.peers); i++ {
            if i == rf.me {
                count++ // 自己也算一票
                continue
            }
            if rf.matchIndex[i] >= N { // 该节点已复制到 N
                count++
            }
        }
        // 多数派已复制 且 该条目属于当前任期（Raft 安全性要求）
        if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
            rf.commitIndex = N // 推进 commitIndex
            break              // 找到最大的 N 即可，立即停止
        }
    }
}

// startOfTerm returns the first log index with the given term, or -1 if not found.
func (rf *Raft) lastIndexOfTerm(term int) int {
    for i := len(rf.log) - 1; i >= 1; i-- {
        if rf.log[i].Term == term {
            return i
        }
    }
    return -1
}

// leader专属函数
func (rf *Raft) stepBack(server int, xTerm, xIndex, xLen int) {
    // 情况1：follower 日志太短
    if xTerm == -1 {
        rf.nextIndex[server] = xLen
        return
    }
    // 情况2：找 leader 日志里有没有 XTerm
    if term := rf.lastIndexOfTerm(xTerm); term != -1 {
        // leader 也有这个 term，冲突在这个 term 的结尾之后，从 start + 1 开始发
        rf.nextIndex[server] = term + 1
    } else {
        // leader 没有这个 term，follower 这个 term 的日志全是错的，从 XIndex 开始覆盖
        rf.nextIndex[server] = xIndex
    }
}
