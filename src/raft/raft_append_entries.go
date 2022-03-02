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
	Term    int
	Success bool

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
	rf.lockMutex()
	defer rf.unlockMutex()
	if rf.killed() {
		return
	}
	rf.dLog("AppendEntries: %+v", args)
	rf.dLog("AppendEntries(): time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.currentRole == Candidate) {
		rf.dLog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		rf.electionResetEvent = time.Now()
		rf.dLog("electionResetEvent in AppendEntries")
		// check if our logEntries contain an entry at PrevLogIndex whose term matches PrevLogTerm
		logOk := rf.logLength() > args.PrevLogIndex &&
			(args.PrevLogIndex == -1 || args.PrevLogTerm == rf.logEntryAtIndex(args.PrevLogIndex).Term)
		if logOk {
			rf.updateLog(args, reply)
		} else {
			rf.updateConflictIndex(args, reply)
		}
	}

	reply.Term = rf.currentTerm
	rf.persist()
	rf.dLog("AppendEntries reply: %+v", *reply)
	rf.dLog("time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))
}

func (rf *Raft) updateLog(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = true

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
	// At the end of this loop:
	// - logInsertIndex points at the end of the logEntries, or an index where the
	//   term mismatches with an entry from the leader
	// - newEntriesIndex points at the end of Entries, or an index where the
	//   term mismatches with the corresponding logEntries entry
	if newEntriesIndex < len(args.Entries) {
		rf.dLog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
		rf.logEntries = append(rf.logEntriesBetween(0, logInsertIndex), args.Entries[newEntriesIndex:]...)
		rf.dLog("... logEntries is now: %v", rf.logEntries)
	}

	// Set commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = intMin(args.LeaderCommit, rf.logLength()-1)
		rf.dLog("... setting commitIndex=%d", rf.commitIndex)
		rf.newApplyReadyCh <- struct{}{}
	}
}

func (rf *Raft) updateConflictIndex(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// No match for PrevLogIndex/PrevLogTerm. Populate
	// ConflictIndex/ConflictTerm to help the leader bring us up to date
	// quickly.
	if args.PrevLogIndex >= rf.logLength() {
		reply.ConflictIndex = rf.logLength()
		reply.ConflictTerm = -1
	} else {
		// PrevLogIndex points within our logEntries, but PrevLogTerm doesn't match rf.logEntries[PrevLogIndex].
		reply.ConflictTerm = rf.logEntryAtIndex(args.PrevLogIndex).Term

		var i int
		for i = args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.logEntryAtIndex(i).Term != reply.ConflictTerm {
				break
			}
		}
		reply.ConflictIndex = i + 1
	}
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// leaderSendAEs sends a round of AEs to all peers, collects their
// replies and adjusts node's state.
func (rf *Raft) leaderSendAEs() {
	rf.lockMutex()
	savedCurrentTerm := rf.currentTerm
	isLeader := rf.currentRole == Leader
	rf.unlockMutex()

	if !isLeader || rf.killed() {
		return
	}

	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(peerId int) {
			rf.replicateLog(peerId, savedCurrentTerm)
		}(peerId)
	}
}

func (rf *Raft) replicateLog(peerId int, savedCurrentTerm int) {
	rf.dLog("Created goroutine from leaderSendAEs for peerId:%d\n", peerId)
	rf.lockMutex()
	ni := rf.nextIndex[peerId]
	entries := rf.logEntriesBetween(ni, rf.logLength())
	rf.dLog("reading nextIndex[%d] = %d", peerId, rf.nextIndex[peerId])
	args := rf.getAppendEntriesArgs(ni, savedCurrentTerm, entries)
	rf.unlockMutex()

	rf.dLog("sending AppendEntries to %v: ni=%d, args=%v", peerId, ni, args)
	var reply AppendEntriesReply
	if len(entries) > 0 {
		rf.dLog("Calling rf.sendAppendEntries with %d entries: %v", len(entries), entries)
	}
	ok := rf.sendAppendEntries(peerId, args, &reply)
	if ok {
		rf.onAppendEntriesReply(peerId, reply, savedCurrentTerm, entries, ni)
	} else {
		rf.dLog("sendAppendEntries failed")
	}
}

// Expects rf.mu to be locked.
func (rf *Raft) getAppendEntriesArgs(ni int, savedCurrentTerm int, entries []LogEntry) AppendEntriesArgs {
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.logEntryAtIndex(prevLogIndex).Term
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

func (rf *Raft) onAppendEntriesReply(peerId int, reply AppendEntriesReply, savedCurrentTerm int, entries []LogEntry, ni int) {
	rf.lockMutex()
	defer rf.unlockMutex()
	if reply.Term > savedCurrentTerm {
		rf.dLog("term out of date in heartbeat reply")
		rf.becomeFollower(reply.Term)
		return
	}

	rf.dLog("currentRole: %v, reply.term: %v", Leader.String(), reply.Term)
	if rf.currentRole == Leader && savedCurrentTerm == reply.Term {
		if reply.Success {
			rf.onAppendEntriesReplySuccess(peerId, entries, ni)
		} else {
			rf.onAppendEntriesReplyFailure(peerId, reply, ni)
		}
	}
}

func (rf *Raft) onAppendEntriesReplyFailure(peerId int, reply AppendEntriesReply, ni int) {
	if reply.ConflictTerm >= 0 {
		lastIndexOfTerm := -1
		for i := rf.logLength() - 1; i >= 0; i-- {
			if rf.logEntryAtIndex(i).Term == reply.ConflictTerm {
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
	rf.dLog("AppendEntries reply from %d not success: nextIndex := %d", peerId, ni-1)
}

func (rf *Raft) onAppendEntriesReplySuccess(peerId int, entries []LogEntry, ni int) {
	if ni+len(entries) > rf.nextIndex[peerId] {
		rf.nextIndex[peerId] = ni + len(entries)
		rf.dLog("On AE reply success nextIndex[%d] = %d", peerId, rf.nextIndex[peerId])
		rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
	}

	savedCommitIndex := rf.commitIndex
	for i := rf.commitIndex + 1; i < rf.logLength(); i++ {
		if rf.logEntryAtIndex(i).Term == rf.currentTerm {
			matchCount := 1
			for peerId := range rf.peers {
				if rf.matchIndex[peerId] >= i {
					matchCount++
				}
			}
			if matchCount*2 > len(rf.peers)+1 {
				rf.commitIndex = i
			}
		}
	}
	rf.persist()
	rf.dLog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, rf.nextIndex, rf.matchIndex, rf.commitIndex)
	if rf.commitIndex != savedCommitIndex {
		rf.dLog("leader sets commitIndex := %d", rf.commitIndex)
		// Commit index changed: the leader considers new entries to be
		// committed. Send new entries on the commit channel to this
		// leader's clients, and notify followers by sending them AEs.
		defer func() {
			if r := recover(); r != nil {
				rf.dLog("Recovered. Error: ", r)
			}
		}()
		rf.newApplyReadyCh <- struct{}{}
	loop:
		for {
			select {
			case rf.triggerAECh <- struct{}{}:
				rf.dLog("Send to triggerAECh channel")
				break loop
			default:
				// heartbeat goroutine will be waiting on the lock before reading from triggerAECh
				// this goroutine will hold the lock and be blocked on writing to triggerAECh which might lead to deadlock
				// we release the lock for a few milliseconds so that heartbeat and this goroutine can continue
				rf.unlockMutex()
				time.Sleep(10 * time.Millisecond)
				rf.lockMutex()
			}
		}
	}
}
