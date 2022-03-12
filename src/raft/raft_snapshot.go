package raft

type InstallSnapshotArgs struct {
	// leader’s term
	Term int

	// so follower can redirect clients
	LeaderId int

	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int

	// term of LastIncludedIndex
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

	rf.dLog("InstallSnapshotArgs: %v, rf.currentTerm: %d, rf.snapshotIndex: %d, rf.lastApplied: %d", args, rf.currentTerm, rf.snapshotIndex, rf.lastApplied)

	if rf.killed() || args.LastIncludedIndex <= rf.snapshotIndex ||
		rf.lastApplied < args.LastIncludedIndex {
		if rf.killed() {
			rf.dLog("rf.killed()")
		}
		if args.LastIncludedIndex <= rf.snapshotIndex {
			rf.dLog("args.LastIncludedIndex <= rf.snapshotIndex, %d, %d", args.LastIncludedIndex, rf.snapshotIndex)
		}
		if rf.lastApplied < args.LastIncludedIndex {
			rf.dLog("rf.lastApplied < args.LastIncludedIndex, %d, %d", rf.lastApplied, args.LastIncludedIndex)
		}
		rf.dLog("returning from InstallSnapshot")
		return
	}
	if rf.currentRole == Leader {
		leastMatchIndex := rf.matchIndex[rf.me]
		leastNextIndex := rf.nextIndex[rf.me]
		for peerId := range rf.peers {
			leastMatchIndex = intMin(leastMatchIndex, rf.matchIndex[peerId])
			leastNextIndex = intMin(leastNextIndex, rf.nextIndex[peerId])
		}
		if leastMatchIndex < args.LastIncludedIndex || leastNextIndex <= args.LastIncludedIndex {
			return
		}
	}

	rf.logEntries = rf.logEntriesBetween(args.LastIncludedIndex+1, rf.logLength())
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.dLog("send snapshot to newApplyReadyCh InstallSnapshot logEntries after change: %v snapshotIndex: %v\n", rf.logEntries, rf.snapshotIndex)
	rf.newApplyReadyCh <- ApplyMsg{CommandValid: false}
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) snapshotToPeers(args InstallSnapshotArgs, savedCurrentTerm int) {
	for peerId := range rf.peers {
		go func(peerId int) {
			reply := InstallSnapshotReply{
				Term: -1,
			}
			rf.dLog("rf.sendInstallSnapshot to peer: %d, args: %v", peerId, args)
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
		}(peerId)
	}
}

func (rf *Raft) onInstallSnapshotReplySuccess(id int, args InstallSnapshotArgs, reply InstallSnapshotReply, term int) {

}
