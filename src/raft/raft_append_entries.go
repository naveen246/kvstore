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
	ConflictTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// AppendEntries RPC handler.
// On receiving AppendEntries request from a leader
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.lockMutex()
	defer rf.unlockMutex()

	rf.dLog("AppendEntries: %+v", args)
	rf.dLog("AppendEntries(): time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.currentRole == Candidate) {
		rf.dLog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	// check if our logEntries contain an entry at PrevLogIndex whose term matches PrevLogTerm
	reply.Success = false
	if args.Term == rf.currentTerm {
		rf.electionResetEvent = time.Now()
		rf.dLog("electionResetEvent in AppendEntries")
		if args.PrevLogIndex < rf.snapshotIndex {
			if rf.snapshotIndex-args.PrevLogIndex >= len(args.Entries) {
				return
			}
			args.Entries = args.Entries[rf.snapshotIndex-args.PrevLogIndex:]
			args.PrevLogIndex = rf.snapshotIndex
			args.PrevLogTerm = rf.snapshotTerm

		}
		prevLogEntry, indexOutOfBoundsErr := rf.logEntryAtIndex(args.PrevLogIndex)
		logOk := rf.logLength() > args.PrevLogIndex &&
			(args.PrevLogIndex == -1 || args.PrevLogTerm == prevLogEntry.Term) && indexOutOfBoundsErr == nil
		if logOk {
			rf.updateLog(args)
			reply.Success = true
			reply.AckMatchIndex = args.PrevLogIndex + len(args.Entries)
		} else {
			reply.ConflictIndex, reply.ConflictTerm = rf.conflictIndexAndTerm(args)
			reply.Success = false
			reply.AckMatchIndex = -1
		}
	}

	reply.Term = rf.currentTerm
	rf.persist()
	rf.dLog("AppendEntries reply: %+v", *reply)
	rf.dLog("time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))
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
		logEntry, _ := rf.logEntryAtIndex(logInsertIndex)
		if logEntry.Term != args.Entries[newEntriesIndex].Term {
			break
		}
		logInsertIndex++
		newEntriesIndex++
	}
	// At the end of this loop:
	// - logInsertIndex points at the end of the logEntries, or an index where the
	//   term mismatches with an entry from the leader
	// - newEntriesIndex points at the end of Entries, or an index where the
	//   term mismatches with the corresponding logEntries entry
	if newEntriesIndex < len(args.Entries) {
		rf.dLog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
		rf.logEntries = append(rf.logEntriesBetween(rf.snapshotLength(), logInsertIndex), args.Entries[newEntriesIndex:]...)
		rf.dLog("... logEntries is now: %v", rf.logEntries)
	}

	// Set commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = intMin(args.LeaderCommit, rf.logLength()-1)
		rf.dLog("... setting commitIndex=%d", rf.commitIndex)
		rf.newApplyReadyCh <- ApplyMsg{CommandValid: true}
	}
}

func (rf *Raft) conflictIndexAndTerm(args AppendEntriesArgs) (int, int) {
	// No match for PrevLogIndex/PrevLogTerm. Populate
	// ConflictIndex/ConflictTerm to help the leader bring us up to date
	// quickly.
	var conflictIndex, conflictTerm int
	if args.PrevLogIndex >= rf.logLength() {
		conflictIndex = rf.logLength()
		conflictTerm = -1
	} else if args.PrevLogIndex == rf.snapshotIndex {
		conflictIndex = rf.snapshotIndex + 1
		conflictTerm = rf.snapshotTerm
	} else {
		// PrevLogIndex points within our logEntries, but PrevLogTerm doesn't match rf.logEntries[PrevLogIndex].
		prevLogEntry, _ := rf.logEntryAtIndex(args.PrevLogIndex)
		conflictTerm = prevLogEntry.Term
		var i int
		for i = args.PrevLogIndex - 1; i >= rf.snapshotIndex; i-- {
			logEntry, _ := rf.logEntryAtIndex(i)
			if logEntry.Term != conflictTerm {
				break
			}
		}
		conflictIndex = i + 1
	}
	return conflictIndex, conflictTerm
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
	rf.dLog("Created goroutine from leaderSendAEs for peerId:%d\n", peerId)
	rf.lockMutex()
	rf.dLog("reading nextIndex[%d] = %d", peerId, rf.nextIndex[peerId])
	args := rf.getAppendEntriesArgs(peerId, leaderCurrentTerm)
	rf.unlockMutex()

	if rf.killed() {
		return
	}
	rf.dLog("sending AppendEntries to %v: args=%v", peerId, args)
	var reply AppendEntriesReply
	if len(args.Entries) > 0 {
		rf.dLog("Calling rf.sendAppendEntries with %d entries: %v", len(args.Entries), args.Entries)
	}
	ok := rf.sendAppendEntries(peerId, args, &reply)
	if ok {
		rf.onAppendEntriesReply(peerId, reply, leaderCurrentTerm)
	} else {
		rf.dLog("sendAppendEntries failed")
	}
}

// Expects rf.mu to be locked.
func (rf *Raft) getAppendEntriesArgs(peerId int, savedCurrentTerm int) AppendEntriesArgs {
	ni := rf.nextIndex[peerId]
	entries := rf.logEntriesBetween(ni, rf.logLength())
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogEntry, _ := rf.logEntryAtIndex(prevLogIndex)
		prevLogTerm = prevLogEntry.Term
	}

	args := AppendEntriesArgs{
		Term:         savedCurrentTerm,
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
	rf.dLog("currentRole: %v, AppendEntriesReply %v", Leader.String(), reply)
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
	if reply.ConflictTerm >= 0 {
		lastIndexOfTerm := -1
		for i := rf.logLength() - 1; i >= rf.snapshotIndex; i-- {
			logEntry, _ := rf.logEntryAtIndex(i)
			if logEntry.Term == reply.ConflictTerm {
				lastIndexOfTerm = i
				break
			}
		}
		if lastIndexOfTerm >= 0 {
			rf.nextIndex[peerId] = lastIndexOfTerm + 1
		} else {
			rf.nextIndex[peerId] = reply.ConflictIndex
		}
	} else {
		rf.nextIndex[peerId] = reply.ConflictIndex
	}
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
		rf.dLog("decreasing nextIndex, reply from peer: %d is %v, rf.matchIndex: %d", peerId, reply, rf.matchIndex[peerId])
		//rf.nextIndex[peerId] -= 1
		rf.unlockMutex()
		return
	}

	rf.dLog("snapshotIndex: %d, logEntries: %v", rf.snapshotIndex, rf.logEntries)
	rf.dLog("On AE reply success commitIndex before change = %d, matchIndex: %v", rf.commitIndex, rf.matchIndex)
	savedCommitIndex := rf.commitIndex
	for i := rf.commitIndex + 1; i < rf.logLength(); i++ {
		logEntry, _ := rf.logEntryAtIndex(i)
		if logEntry.Term == rf.currentTerm {
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
	rf.dLog("On AE reply success commitIndex after change = %d, matchIndex: %v", rf.commitIndex, rf.matchIndex)
	rf.persist()
	rf.dLog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, rf.nextIndex, rf.matchIndex, rf.commitIndex)

	if rf.commitIndex != savedCommitIndex {
		rf.dLog("leader sets commitIndex := %d", rf.commitIndex)
		// Commit index changed: the leader considers new entries to be
		// committed. Send new entries on the commit channel to this
		// leader's clients, and notify followers by sending them AEs.
		rf.newApplyReadyCh <- ApplyMsg{CommandValid: true}
		rf.unlockMutex()
		rf.triggerAECh <- struct{}{}
		rf.dLog("Sent to triggerAECh channel")
		return
	}
	rf.unlockMutex()
}
