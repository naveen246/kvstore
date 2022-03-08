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

	if rf.killed() || args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.snapshotIndex ||
		rf.lastApplied < args.LastIncludedIndex {
		return
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.currentRole == Candidate) {
		rf.dLog("... term out of date in InstallSnapshot")
		rf.becomeFollower(args.Term)
	}
	// If existing log entry has the same index and term as snapshot’s last included entry,
	//		retain log entries following it and reply
	// else
	//		Discard the entire log

	rf.dLog("Before changing logs for snapshot\n")
	rf.dLog("args.LastIncludedIndex: %d\targs.LastIncludedTerm: %d\trf.logEntries: %v\trf.snapshotIndex: %d\n",
		args.LastIncludedIndex, args.LastIncludedTerm, rf.logEntries, rf.snapshotIndex)
	rf.dLog("rf.logEntries before snapshot change: %v\n", rf.logEntries)

	if args.LastIncludedIndex >= rf.logLength() {
		rf.logEntries = []LogEntry{}
	} else {
		rf.logEntries = rf.logEntriesBetween(args.LastIncludedIndex+1, rf.logLength())
	}
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
	}
}

func (rf *Raft) onInstallSnapshotReplySuccess(peerId int, args InstallSnapshotArgs, reply InstallSnapshotReply, savedCurrentTerm int) {
	rf.lockMutex()
	defer rf.unlockMutex()
	if rf.currentRole == Leader {
		if args.LastIncludedIndex > rf.matchIndex[peerId] {
			rf.matchIndex[peerId] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peerId] {
			rf.nextIndex[peerId] = args.LastIncludedIndex + 1
		}
	}
	if rf.lastApplied < rf.snapshotIndex {
		rf.newApplyReadyCh <- ApplyMsg{CommandValid: false}
	}
	rf.dLog("rf.onInstallSnapshotReplySuccess from peer: %d, args: %v, matchIndex[peer]: %d, nextIndex[peer]: %d\nlastApplied: %d, snapShotIndex: %d", peerId, args, rf.matchIndex[peerId], rf.nextIndex[peerId], rf.lastApplied, rf.snapshotIndex)
}
