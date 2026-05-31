package raft


// 这些全部线程不安全的！
// I do not lock in helpers! as in 2A!





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
    for N := rf.logLength() - 1; N > rf.commitIndex && N > rf.snapIndex; N-- {
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
        if count > len(rf.peers)/2 && rf.get(N).Term == rf.currentTerm {
            rf.commitIndex = N // 推进 commitIndex
            rf.bEffortKick()
            break              // 找到最大的 N 即可，立即停止
        }
    }
}





// lastOfTerm returns the last log index with the given term, or -1 if not found.
func (rf *Raft) lastIndexOfTerm(term int) int {
    for i := rf.logLength() - 1; i > rf.snapIndex; i-- {
        if rf.get(i).Term == term {
            return i
        }
    }
    return -1
}

// leader专属函数
func (rf *Raft) stepBack(server int, xTerm, xIndex, xLen int) {
    // 情况1：follower 日志太短
    if xTerm == -1 {
        rf.nextIndex[server] = min(xLen, rf.logLength())
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

// reconcileEntries 从 myIdx（本地日志全局index）和 yourIdx（entries切片下标）开始，
// 将incoming entries与本地日志逐条比对：遇到term冲突则截断本地日志，
// 最后将剩余未处理的entries追加到本地日志。
func (rf *Raft) reconcileEntries(myIdx int, yourIdx int, entries []Entry) {
    for myIdx < rf.logLength() && yourIdx < len(entries) {
        if rf.get(myIdx).Term != entries[yourIdx].Term {
            rf.log = rf.log[:myIdx - rf.snapIndex]
            break
        }
        myIdx++
        yourIdx++
    }
    rf.batchAppend(entries[yourIdx:])
}

func (rf *Raft) tryUpdateCommit(leaderCommit int) {
    if leaderCommit > rf.commitIndex {
        rf.commitIndex = min(leaderCommit, rf.logLength() - 1)
        if rf.commitIndex > rf.lastApplied {
            rf.bEffortKick()
        }
    }
}




