package raft  



func (rf *Raft) getSnap() int {
    return rf.log[0].Term
}

func (rf *Raft) setSnap(snapTerm int) {
    rf.log[0].Term = snapTerm
    
}











func (rf *Raft) get(i int) Entry {
    return rf.log[i - rf.snapIndex]
}

func (rf *Raft) entriesFrom(start int) []Entry {
    return append([]Entry{}, rf.log[start - rf.snapIndex:]...)
}

func (rf *Raft) logLength() int {
    return len(rf.log) + rf.snapIndex
}
















func (rf *Raft) append(entry Entry) {

	rf.log = append(rf.log, entry)
	rf.bEffortKick()
	rf.persist()
}

func (rf *Raft) batchAppend(entries []Entry) {
    rf.log = append(rf.log, entries...)
    rf.bEffortKick()
    rf.persist()
}