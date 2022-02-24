package raft

import "time"

//
// RequestVoteArgs : RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply : RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// RequestVote RPC handler.
// On receiving RequestVote from a candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.lockMutex()
	defer rf.unlockMutex()
	if rf.killed() {
		return
	}
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	rf.dLog("RequestVote: %+v [currentTerm=%d, votedFor=%d, logEntries index/term=(%d, %d)]", args, rf.currentTerm, rf.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > rf.currentTerm {
		rf.dLog("... term out of date in RequestVote")
		rf.becomeFollower(args.Term)
	}

	logOk := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	termOk := args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)

	if logOk && termOk {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionResetEvent = time.Now()
		rf.dLog("electionResetEvent in RequestVote")
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.persist()
	rf.dLog("... RequestVote reply: %+v", reply)
}

// startElection starts a new election with this node as a candidate.
// Expects rf.mu to be locked.
func (rf *Raft) startElection() {
	candidateCurrentTerm := rf.becomeCandidate()
	votesReceived := 1

	// Send RequestVote RPCs to all other servers concurrently.
	for id := range rf.peers {
		go func(peerId int, votesReceived *int) {
			args := rf.getRequestVoteArgs(candidateCurrentTerm)
			rf.dLog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peerId, &args, &reply)
			if ok {
				rf.onRequestVoteReply(reply, candidateCurrentTerm, votesReceived)
			} else {
				rf.dLog("sendRequestVote failed")
			}
		}(id, &votesReceived)
	}

	// Run another election ticker, in case this election is not successful.
	go rf.ticker()
}

func (rf *Raft) getRequestVoteArgs(savedCurrentTerm int) RequestVoteArgs {
	rf.lockMutex()
	defer rf.unlockMutex()
	savedLastLogIndex, savedLastLogTerm := rf.lastLogIndexAndTerm()

	args := RequestVoteArgs{
		Term:         savedCurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: savedLastLogIndex,
		LastLogTerm:  savedLastLogTerm,
	}
	return args
}

func (rf *Raft) onRequestVoteReply(reply RequestVoteReply, candidateCurrentTerm int, votesReceived *int) {
	rf.lockMutex()
	defer rf.unlockMutex()
	if rf.state != Candidate {
		rf.dLog("while waiting for reply, state = %v", rf.state)
		return
	}

	if reply.Term > candidateCurrentTerm {
		rf.dLog("term out of date in RequestVoteReply")
		rf.becomeFollower(reply.Term)
	} else if reply.Term == candidateCurrentTerm && reply.VoteGranted {
		*votesReceived += 1
		if *votesReceived*2 > len(rf.peers)+1 {
			rf.dLog("wins election with %d votes", *votesReceived)
			rf.startLeader()
		}
	}
}
