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
            rf.kick()
            break              // 找到最大的 N 即可，立即停止
        }
    }
}




