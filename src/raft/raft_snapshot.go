package raft

// InstallSnapshotArgs Figure 13 from Raft paper
type InstallSnapshotArgs struct {
	// leader’s term
	Term int

	// so follower can redirect clients
	LeaderId int

	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int

	// term of lastIncludedIndex
	LastIncludedTerm int

	// raw bytes of the snapshot
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// InstallSnapshot Invoked by leader to send chunks of a snapshot to a follower.
// On receiving snapshot from leader
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lockMutex()
	defer rf.unlockMutex()

	if rf.killed() || args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}
	if args.Term > rf.currentTerm {
		rf.dLog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}
	// If existing log entry has the same index and term as snapshot’s last included entry,
	//		retain log entries following it and reply
	// else
	//		Discard the entire log

	rf.dLog("[%d] Before changing logs for snapshot\n", rf.me)
	rf.dLog("args.LastIncludedIndex: %d\targs.LastIncludedTerm: %d\trf.logEntries: %v\trf.snapshotIndex: %d\n",
		args.LastIncludedIndex, args.LastIncludedTerm, rf.logEntries, rf.snapshotIndex)
	if rf.lastApplied < args.LastIncludedIndex {
		return
	}
	if args.LastIncludedIndex > rf.snapshotIndex {
		rf.dLog("[%d] - rf.logEntries before snapshot change: %v\n", rf.me, rf.logEntries)
		rf.snapshotTerm = rf.logEntryAtIndex(args.LastIncludedIndex).Term
		rf.logEntries = rf.logEntriesBetween(args.LastIncludedIndex+1, rf.logLength())
		rf.snapshot = args.Data
		rf.snapshotIndex = args.LastIncludedIndex
		rf.dLog("[%d] - send snapshot to newApplyReadyCh InstallSnapshot logEntries after change: %v snapshotIndex: %v\n", rf.me, rf.logEntries, rf.snapshotIndex)
		rf.newApplyReadyCh <- ApplyMsg{CommandValid: false}
		reply.Term = rf.currentTerm
		rf.persist()
	}
}

func (rf *Raft) snapshotToPeers(args InstallSnapshotArgs, savedCurrentTerm int) {
	for peerId := range rf.peers {
		reply := InstallSnapshotReply{
			Term: -1,
		}
		ok := rf.sendInstallSnapshot(peerId, &args, &reply)
		if ok && reply.Term != -1 {
			if reply.Term > savedCurrentTerm {
				rf.lockMutex()
				rf.becomeFollower(reply.Term)
				rf.unlockMutex()
				return
			}
			rf.onInstallSnapshotReplySuccess(peerId, args, reply, savedCurrentTerm)
		}
	}
	return
}

func (rf *Raft) onInstallSnapshotReplySuccess(peerId int, args InstallSnapshotArgs, reply InstallSnapshotReply, savedCurrentTerm int) {
	//rf.lockMutex()
	//defer rf.unlockMutex()
	//ni := rf.snapshotIndex + len(rf.logEntries) + 1
	//if ni > rf.nextIndex[peerId] {
	//	rf.nextIndex[peerId] = ni
	//	rf.matchIndex[peerId] = ni - 1
	//}

	//for i := rf.commitIndex + 1; i < rf.logLength(); i++ {
	//	if rf.logEntryAtIndex(i).Term == rf.currentTerm {
	//		matchCount := 1
	//		for peerId := range rf.peers {
	//			if rf.matchIndex[peerId] >= i {
	//				matchCount++
	//			}
	//		}
	//		if matchCount*2 > len(rf.peers)+1 {
	//			rf.commitIndex = i
	//		}
	//	}
	//}
}
