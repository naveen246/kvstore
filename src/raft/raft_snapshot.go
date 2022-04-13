package raft

import "fmt"

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
	reply.Term = rf.currentTerm
	rf.dLog("InstallSnapshotArgs: %+v, rf.currentTerm: %d, rf.snapshotIndex: %d, rf.lastApplied: %d", InstallSnapshotArgsToStr(*args), rf.currentTerm, rf.snapshotIndex, rf.lastApplied)

	if rf.killed() || args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.snapshotIndex {
		rf.dLog("returning from InstallSnapshot")
		return
	}

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.currentRole == Candidate) {
		rf.dLog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	rf.logEntries = rf.logEntriesBetween(args.LastIncludedIndex+1, rf.logLength())
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.dLog("send snapshot to snapshotReadyCh InstallSnapshot logEntries after change: %+v snapshotIndex: %+v\n", rf.logEntries, rf.snapshotIndex)
	rf.snapshotReadyCh <- struct{}{}

	rf.persist()
}

func (rf *Raft) snapshotToPeer(peerId int, snapshotIndex int, snapshotTerm int, snapshot []byte) {
	rf.lockMutex()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  snapshotTerm,
		Data:              snapshot,
	}
	rf.dLog("rf.snapshotToPeer, peerId: %d, InstallSnapshotArgs: %+v, rf.matchIndex: %+v, rf.snapshotIndex: %d", peerId, InstallSnapshotArgsToStr(args), rf.matchIndex, rf.snapshotIndex)
	rf.unlockMutex()
	reply := InstallSnapshotReply{
		Term: -1,
	}
	rf.dLog("rf.sendInstallSnapshot to peer: %d, args: %+v", peerId, InstallSnapshotArgsToStr(args))
	ok := rf.sendInstallSnapshot(peerId, &args, &reply)
	if ok && reply.Term != -1 {
		rf.onInstallSnapshotReply(peerId, args, reply)
	}
}

func (rf *Raft) onInstallSnapshotReply(peerId int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	rf.lockMutex()
	defer rf.unlockMutex()
	rf.dLog("onInstallSnapshotReply - peer: %d, InstallSnapshotArgs: %+v, InstallSnapshotReply: %+v, rf.currentTerm: %d, rf.matchIndex: %+v, rf.nextIndex: %+v", peerId, InstallSnapshotArgsToStr(args), reply, rf.currentTerm, rf.matchIndex, rf.nextIndex)
	if reply.Term > rf.currentTerm || rf.currentRole != Leader {
		rf.becomeFollower(reply.Term)
		return
	}
	if args.LastIncludedIndex > rf.matchIndex[peerId] {
		rf.matchIndex[peerId] = args.LastIncludedIndex
		rf.nextIndex[peerId] = args.LastIncludedIndex + 1
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peerId] {
		rf.nextIndex[peerId] = args.LastIncludedIndex + 1
	}
	rf.dLog("onInstallSnapshotReply - peer: %d, InstallSnapshotArgs: %+v, InstallSnapshotReply: %+v, rf.matchIndex: %+v, rf.nextIndex: %+v", peerId, InstallSnapshotArgsToStr(args), reply, rf.matchIndex, rf.nextIndex)
}

func InstallSnapshotArgsToStr(args InstallSnapshotArgs) string {
	return fmt.Sprintf("%+v", InstallSnapshotArgs{
		Term:              args.Term,
		LeaderId:          args.LeaderId,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
	})
}