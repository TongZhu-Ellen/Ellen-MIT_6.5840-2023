package raft 


// Entry制造入口！
func (rf *Raft) Start(command interface{}) (int, int, bool) {


	rf.mu.Lock()
	defer rf.mu.Unlock()


	if rf.state != Leader {
		return -1, -1, false
	}

	entry := Entry{
		Term: rf.currentTerm,
		Command: command,
	}
	
	
	rf.append(entry)

	index := rf.logLength() - 1 // index to be inserted to! 
	term := rf.currentTerm
	isLeader := rf.state == Leader

	return index, term, isLeader
}

func (rf *Raft) bEffortKick() {
    // "只要已经有人通知过了，就不需要再重复通知"
	select {
	case rf.wakeCh <- struct{}{}:
	default:
	}
    
}

// Make raft的时候就要打开！
func (rf *Raft) guardKick() {
	for range rf.wakeCh {

		rf.mu.Lock() // -------------- 锁！----------
		start := rf.lastApplied + 1
		end := rf.commitIndex
		if end < start {
			rf.mu.Unlock()
			continue
		}
		applies := make([]ApplyMsg, end+1-start)
		j := 0 // rel-index

		for i := start; i <= end; i++ {
			if i <= rf.snapIndex {  // 已被快照覆盖，跳过
				continue
			}
			applies[j] = ApplyMsg{
				CommandValid: true,
				Command: rf.get(i).Command,
				CommandIndex: i,
			}
			j++
		}

		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock() // -------------- 锁！----------

		for _, apply := range applies[:j] {
			rf.applyCh <- apply
		}


	}
}
































