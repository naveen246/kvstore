package raft

import "time"

// AppendEntriesArgs See figure 2 in the paper.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	AckMatchIndex int

	ConflictIndex int
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// AppendEntries RPC handler.
// On receiving AppendEntries request from a leader
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lockMutex()
	defer rf.unlockMutex()
	if rf.killed() {
		return
	}
	rf.dLog("AppendEntries: %+v", args)
	rf.dLog("AppendEntries(): time elapsed since electionResetEvent - %+v", time.Since(rf.electionResetEvent))

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.currentRole == Candidate) {
		rf.dLog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	// check if our logEntries contain an entry at PrevLogIndex whose term matches PrevLogTerm
	reply.Success = false
	if args.Term == rf.currentTerm {
		rf.electionResetEvent = time.Now()
		rf.dLog("electionResetEvent in AppendEntries")
		logOk := rf.logLength() > args.PrevLogIndex &&
			(args.PrevLogIndex == -1 || args.PrevLogTerm == rf.logEntryAtIndex(args.PrevLogIndex).Term)
		if logOk {
			rf.updateLog(args)
			reply.Success = true
			reply.AckMatchIndex = args.PrevLogIndex + len(args.Entries)
		} else {
			reply.ConflictIndex = rf.conflictIndex(args)
			reply.Success = false
			reply.AckMatchIndex = -1
		}
	}

	reply.Term = rf.currentTerm
	rf.persist()
	rf.dLog("AppendEntries reply: %+v", *reply)
	rf.dLog("time elapsed since electionResetEvent - %+v", time.Since(rf.electionResetEvent))
}

func (rf *Raft) updateLog(args AppendEntriesArgs) {
	// Find an insertion point - where there's a term mismatch between
	// the existing logEntries starting at PrevLogIndex+1 and the new entries sent
	// in the RPC.
	logInsertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for {
		if logInsertIndex >= rf.logLength() || newEntriesIndex >= len(args.Entries) {
			break
		}
		if rf.logEntryAtIndex(logInsertIndex).Term != args.Entries[newEntriesIndex].Term {
			break
		}
		logInsertIndex++
		newEntriesIndex++
	}
	if rf.killed() {
		return
	}
	// At the end of this loop:
	// - logInsertIndex points at the end of the logEntries, or an index where the
	//   term mismatches with an entry from the leader
	// - newEntriesIndex points at the end of Entries, or an index where the
	//   term mismatches with the corresponding logEntries entry
	if newEntriesIndex < len(args.Entries) {
		rf.dLog("... inserting entries %+v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
		rf.logEntries = append(rf.logEntriesBetween(0, logInsertIndex), args.Entries[newEntriesIndex:]...)
		rf.dLog("... logEntries is now: %+v", rf.logEntries)
	}

	// Set commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = intMin(args.LeaderCommit, rf.logLength()-1)
		rf.dLog("... setting commitIndex=%d", rf.commitIndex)
		rf.commandReadyCh <- struct{}{}
	}
}

//When rejecting an AppendEntries request, the follower
//can include the first index of the term for which there is a conflicting entry. With this information, the
//leader can decrement nextIndex to bypass all the conflicting entries in that term; one AppendEntries RPC will
//be required for each term with conflicting entries, rather
//than one RPC per entry.
func (rf *Raft) conflictIndex(args AppendEntriesArgs) int {
	// No match for PrevLogIndex/PrevLogTerm. Populate
	// ConflictIndex to help the leader bring us up to date quickly.
	var conflictIndex int
	if args.PrevLogIndex >= rf.logLength() {
		conflictIndex = rf.logLength()
	} else {
		// PrevLogIndex points within our logEntries, but PrevLogTerm doesn't match rf.logEntries[PrevLogIndex].
		conflictTerm := rf.logEntryAtIndex(args.PrevLogIndex).Term
		var i int
		for i = args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.logEntryAtIndex(i).Term != conflictTerm {
				break
			}
		}
		conflictIndex = i + 1
	}
	return conflictIndex
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// leaderSendAEs sends a round of AEs to all peers, collects their
// replies and adjusts node's state.
func (rf *Raft) leaderSendAEs(currentTerm int) {
	if rf.killed() {
		return
	}
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(peerId int) {
			rf.replicateLog(peerId, currentTerm)
		}(peerId)
	}
}

func (rf *Raft) replicateLog(peerId int, leaderCurrentTerm int) {
	if rf.killed() {
		return
	}
	rf.dLog("Created goroutine from leaderSendAEs for peerId:%d\n", peerId)
	rf.lockMutex()
	rf.dLog("reading nextIndex[%d] = %d", peerId, rf.nextIndex[peerId])
	args := rf.getAppendEntriesArgs(peerId, leaderCurrentTerm)
	rf.dLog("sending AppendEntries to %+v: ni=%d, args=%+v", peerId, rf.nextIndex[peerId], args)
	rf.unlockMutex()

	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peerId, args, &reply)
	if ok {
		rf.onAppendEntriesReply(peerId, reply, leaderCurrentTerm)
	} else {
		rf.dLog("sendAppendEntries failed")
	}
}

// Expects rf.mu to be locked.
func (rf *Raft) getAppendEntriesArgs(peerId int, leaderCurrentTerm int) AppendEntriesArgs {
	entries := rf.logEntriesBetween(rf.nextIndex[peerId], rf.logLength())
	prevLogIndex := rf.nextIndex[peerId] - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.logEntryAtIndex(prevLogIndex).Term
	}

	args := AppendEntriesArgs{
		Term:         leaderCurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) onAppendEntriesReply(peerId int, reply AppendEntriesReply, savedCurrentTerm int) {
	if rf.killed() {
		return
	}
	rf.lockMutex()
	if reply.Term > savedCurrentTerm {
		rf.dLog("term out of date in heartbeat reply")
		rf.becomeFollower(reply.Term)
		rf.unlockMutex()
		return
	}
	isLeader := rf.currentRole == Leader
	rf.unlockMutex()
	rf.dLog("currentRole: %+v, reply.term: %+v", Leader.String(), reply.Term)
	if reply.Term == savedCurrentTerm && isLeader {
		if reply.Success {
			rf.onAppendEntriesReplySuccess(peerId, reply)
		} else {
			rf.onAppendEntriesReplyFailure(peerId, reply)
		}
	}
}

func (rf *Raft) onAppendEntriesReplyFailure(peerId int, reply AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.lockMutex()
	defer rf.unlockMutex()
	rf.nextIndex[peerId] = reply.ConflictIndex
}

func (rf *Raft) onAppendEntriesReplySuccess(peerId int, reply AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.lockMutex()
	if reply.AckMatchIndex >= rf.matchIndex[peerId] {
		rf.nextIndex[peerId] = reply.AckMatchIndex + 1
		rf.dLog("On AE reply success nextIndex[%d] = %d", peerId, rf.nextIndex[peerId])
		rf.matchIndex[peerId] = reply.AckMatchIndex
	} else {
		rf.nextIndex[peerId] -= 1
		rf.unlockMutex()
		return
	}

	savedCommitIndex := rf.commitIndex
	for i := rf.commitIndex + 1; i < rf.logLength(); i++ {
		if rf.logEntryAtIndex(i).Term == rf.currentTerm {
			matchCount := 0
			for peerId := range rf.peers {
				if rf.matchIndex[peerId] >= i {
					matchCount++
				}
			}
			if matchCount*2 >= len(rf.peers)+1 {
				rf.commitIndex = i
			}
		}
	}
	rf.persist()
	rf.dLog("AppendEntries reply from %d success: nextIndex := %+v, matchIndex := %+v; commitIndex := %d", peerId, rf.nextIndex, rf.matchIndex, rf.commitIndex)

	if rf.commitIndex != savedCommitIndex {
		rf.dLog("leader sets commitIndex := %d", rf.commitIndex)
		// Commit index changed: the leader considers new entries to be
		// committed. Send new entries on the commit channel to this
		// leader's clients, and notify followers by sending them AEs.
		rf.commandReadyCh <- struct{}{}
		rf.unlockMutex()
		rf.triggerAECh <- struct{}{}
		rf.dLog("Sent to triggerAECh channel")
		return
	}
	rf.unlockMutex()
}
