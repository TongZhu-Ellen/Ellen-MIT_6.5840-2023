package raft 



type InstallSnapshotArgs struct {
    Term              int
    LeaderId          int
    LastIncludedIndex int
    LastIncludedTerm  int
    Data              []byte
}

type InstallSnapshotReply struct {
    Term int
}





func (rf *Raft) helpInstall(i int) {
    args := &InstallSnapshotArgs{
        Term:              rf.currentTerm,
        LeaderId:          rf.me,
        LastIncludedIndex: rf.snapIndex,
        LastIncludedTerm:  rf.getSnap(),
        Data:              rf.snapshot,
    }
    reply := &InstallSnapshotReply{}
    rf.mu.Unlock() // 发 RPC 前放锁

    
    ok := rf.sendInstallSnapshot(i, args, reply)

    rf.mu.Lock() // 发完再拿锁
    if !ok { return }
    if reply.Term > rf.currentTerm {
        rf.newGen(reply.Term)
        return
    }
    rf.matchIndex[i] = args.LastIncludedIndex
    rf.nextIndex[i] = args.LastIncludedIndex + 1
}


func (rf *Raft) sendInstallSnapshot(i int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
    return ok
}













func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    }
    if args.Term > rf.currentTerm {
        rf.newGen(args.Term)
    }
    rf.touched()

    if args.LastIncludedIndex <= rf.snapIndex {
        reply.Term = rf.currentTerm
        return
    }

    // trim log
    newLog := []Entry{{}}
    if args.LastIncludedIndex < rf.logLength()-1 {
        newLog = append(newLog, rf.entriesFrom(args.LastIncludedIndex+1)...)
    }
    rf.log = newLog
    rf.snapIndex = args.LastIncludedIndex
    rf.setSnap(args.LastIncludedTerm)
    rf.snapshot = args.Data
    rf.persist()

    if rf.lastApplied < rf.snapIndex {
        rf.lastApplied = rf.snapIndex
        rf.commitIndex = rf.snapIndex
        go func() {
            rf.applyCh <- ApplyMsg{
                SnapshotValid: true,
                Snapshot:      args.Data,
                SnapshotTerm:  args.LastIncludedTerm,
                SnapshotIndex: args.LastIncludedIndex,
            }
        }()
    }

    reply.Term = rf.currentTerm
}


