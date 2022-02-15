package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const DebugMode = false

const (
	ElectionTimeout  = 300 * time.Millisecond
	HeartBeatTimeout = 200 * time.Millisecond
	ElectionTicker   = 10 * time.Millisecond
)

//
// ApplyMsg : as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic("unreachable")
	}
}

func logFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

//
// Raft : A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// applyCh is the channel where this node is going to report committed log
	// entries. It's passed in by the client during construction.
	applyCh chan<- ApplyMsg

	// newApplyReadyCh is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on applyCh.
	newApplyReadyCh chan struct{}

	// triggerAECh is an internal notification channel used to trigger
	// sending new AEs to followers when interesting changes occurred.
	triggerAECh chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile raft state on all servers
	commitIndex        int
	lastApplied        int
	state              NodeState
	electionResetEvent time.Time

	// Volatile raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects rf.mu to be locked.
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log) - 1
		lastTerm := rf.log[lastIndex].Term
		return lastIndex, lastTerm
	}
	return -1, -1
}

// GetState : return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.lockMutex()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.unlockMutex()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Expects rf.mu to be locked.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(rf.currentTerm)
	logFatal(err)
	err = e.Encode(rf.votedFor)
	logFatal(err)
	err = e.Encode(rf.log)
	logFatal(err)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.dLog("persist(): time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))
}

//
// restore previously persisted state.
// Expects rf.mu to be locked.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		logFatal(errors.New("error while decoding persisted data"))
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot : the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.lockMutex()
	defer rf.unlockMutex()
	if rf.killed() {
		return
	}
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	rf.dLog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, rf.currentTerm, rf.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > rf.currentTerm {
		rf.dLog("... term out of date in RequestVote")
		rf.becomeFollower(args.Term)
	}

	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
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

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	ConflictIndex int
	ConflictTerm  int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lockMutex()
	defer rf.unlockMutex()
	if rf.killed() {
		return
	}
	rf.dLog("AppendEntries: %+v", args)
	rf.dLog("AppendEntries(): time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))

	if args.Term > rf.currentTerm {
		rf.dLog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if args.LeaderId != rf.me && rf.state != Follower {
			rf.becomeFollower(args.Term)
		}
		rf.electionResetEvent = time.Now()
		rf.dLog("electionResetEvent in AppendEntries")
		// check if our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				rf.dLog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				rf.dLog("... log is now: %v", rf.log)
			}

			// Set commitIndex
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = intMin(args.LeaderCommit, len(rf.log)-1)
				rf.dLog("... setting commitIndex=%d", rf.commitIndex)
				rf.newApplyReadyCh <- struct{}{}
			}
		} else {
			// No match for PrevLogIndex/PrevLogTerm. Populate
			// ConflictIndex/ConflictTerm to help the leader bring us up to date
			// quickly.
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictIndex = len(rf.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex points within our log, but PrevLogTerm doesn't match
				// rf.log[PrevLogIndex].
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}

	}

	reply.Term = rf.currentTerm
	rf.persist()
	rf.dLog("AppendEntries reply: %+v", *reply)
	rf.dLog("time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// Start agreement on the next command
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.lockMutex()
	if rf.state == Leader && !rf.killed() {
		rf.dLog("Start agreement on next command: %v\t log: %v at node %v", command, rf.log, rf.me)
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		})
		rf.persist()
		rf.dLog("... log=%v", rf.log)
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true
		rf.unlockMutex()
		rf.triggerAECh <- struct{}{}
		return index, term, isLeader
	}
	rf.unlockMutex()
	return index, term, isLeader
}

//
// Kill node the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.dLog("node dead")
	close(rf.newApplyReadyCh)
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// electionTimeout generates a pseudo-random election timeout duration.
func (rf *Raft) electionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	timeoutDuration := rf.electionTimeout()
	rf.lockMutex()
	termStarted := rf.currentTerm
	rf.unlockMutex()
	rf.dLog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// This loops until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this raft node becomes a candidate
	electionTicker := time.NewTicker(ElectionTicker)
	defer electionTicker.Stop()
	for rf.killed() == false {
		// Your code here to check if a leader election should be started
		<-electionTicker.C
		rf.lockMutex()
		if rf.state != Candidate && rf.state != Follower {
			rf.dLog("in election timer state=%s, bailing out", rf.state)
			rf.unlockMutex()
			return
		}

		if termStarted != rf.currentTerm {
			rf.dLog("in election timer term changed from %d to %d, bailing out", termStarted, rf.currentTerm)
			rf.unlockMutex()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		elapsed := time.Since(rf.electionResetEvent)
		if elapsed >= timeoutDuration {
			rf.dLog("elapsed: %v, timeoutDuration: %v, electionResetEvent: %v", elapsed, timeoutDuration, rf.electionResetEvent)
			rf.startElection()
			rf.unlockMutex()
			return
		}
		rf.unlockMutex()
	}
}

// startElection starts a new election with this node as a candidate.
// Expects rf.mu to be locked.
func (rf *Raft) startElection() {
	rf.dLog("startElection current state: %v", rf.state)
	rf.state = Candidate
	rf.currentTerm += 1
	savedCurrentTerm := rf.currentTerm
	rf.electionResetEvent = time.Now()
	rf.dLog("electionResetEvent in startElection")
	rf.votedFor = rf.me
	rf.dLog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, rf.log)

	votesReceived := 1

	// Send RequestVote RPCs to all other servers concurrently.
	for id := range rf.peers {
		go func(peerId int) {
			rf.lockMutex()
			savedLastLogIndex, savedLastLogTerm := rf.lastLogIndexAndTerm()
			rf.unlockMutex()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			rf.dLog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peerId, &args, &reply)
			if ok {
				rf.lockMutex()
				defer rf.unlockMutex()
				if rf.state != Candidate {
					rf.dLog("while waiting for reply, state = %v", rf.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					rf.dLog("term out of date in RequestVoteReply")
					rf.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(rf.peers)+1 {
							rf.dLog("wins election with %d votes", votesReceived)
							rf.startLeader()
							return
						}
					}
				}
			} else {
				rf.dLog("sendRequestVote failed")
			}
		}(id)
	}

	// Run another election ticker, in case this election is not successful.
	go rf.ticker()
}

// becomeFollower makes raft node a follower and resets its state.
// Expects rf.mu to be locked.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.dLog("becomes Follower with term=%d; log=%v", term, rf.log)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionResetEvent = time.Now()
	rf.dLog("electionResetEvent in becomeFollower")
	go rf.ticker()
}

// startLeader switches node into a leader state and begins process of heartbeats.
// Expects rf.mu to be locked.
func (rf *Raft) startLeader() {
	rf.state = Leader

	for peerId := range rf.peers {
		rf.nextIndex[peerId] = len(rf.log)
		rf.matchIndex[peerId] = -1
	}
	rf.dLog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", rf.currentTerm, rf.nextIndex, rf.matchIndex, rf.log)

	// This goroutine runs in the background and sends AEs to peers:
	// * Whenever something is sent on triggerAECh
	// * ... Or every 200 ms, if no events occur on triggerAECh
	go func(heartbeatTimeout time.Duration) {
		rf.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true
				rf.dLog("heartbeat")
				// Reset timer to fire again after heartbeatTimeout.
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-rf.triggerAECh:
				if ok {
					doSend = true
					rf.dLog("heartbeat: read on triggerAECh")
				} else {
					rf.dLog("heartbeat: return")
					return
				}

				// Reset timer for heartbeatTimeout.
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				rf.dLog("doSend AppendEntries from leader to peers")
				rf.lockMutex()
				if rf.state != Leader {
					rf.unlockMutex()
					return
				}
				rf.unlockMutex()
				rf.dLog("Call leaderSendAEs inside heartbeat: time elapsed since electionResetEvent - %v", time.Since(rf.electionResetEvent))
				rf.leaderSendAEs()
			}
		}
	}(HeartBeatTimeout)
}

// leaderSendAEs sends a round of AEs to all peers, collects their
// replies and adjusts node's state.
func (rf *Raft) leaderSendAEs() {
	rf.lockMutex()
	savedCurrentTerm := rf.currentTerm
	rf.unlockMutex()

	for peerId := range rf.peers {
		go func(peerId int) {
			rf.lockMutex()
			ni := rf.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			entries := rf.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.unlockMutex()
			rf.dLog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(peerId, args, &reply)
			rf.dLog("Called rf.sendAppendEntries")
			if ok {
				if reply.Term > savedCurrentTerm {
					rf.dLog("term out of date in heartbeat reply")
					rf.lockMutex()
					rf.becomeFollower(reply.Term)
					rf.unlockMutex()
					return
				}

				rf.dLog("state: %v, reply.term: %v", Leader.String(), reply.Term)
				rf.lockMutex()
				if rf.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						rf.nextIndex[peerId] = ni + len(entries)
						rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1

						savedCommitIndex := rf.commitIndex
						for i := rf.commitIndex + 1; i < len(rf.log); i++ {
							if rf.log[i].Term == rf.currentTerm {
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
						rf.dLog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, rf.nextIndex, rf.matchIndex, rf.commitIndex)
						if rf.commitIndex != savedCommitIndex {
							rf.dLog("leader sets commitIndex := %d", rf.commitIndex)
							// Commit index changed: the leader considers new entries to be
							// committed. Send new entries on the commit channel to this
							// leader's clients, and notify followers by sending them AEs.
							rf.newApplyReadyCh <- struct{}{}
							for {
								select {
								case rf.triggerAECh <- struct{}{}:
									break
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
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
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
						rf.dLog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
				rf.unlockMutex()
			} else {
				rf.dLog("sendAppendEntries failed")
			}
		}(peerId)
	}
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// dLog logs a debugging message if DebugMode is true.
func (rf *Raft) dLog(format string, args ...interface{}) {
	if DebugMode {
		format = fmt.Sprintf("[%v]\t\t[%d]\t", getGID(), rf.me) + format
		log.Printf(format, args...)
	}
}

// applyChSender is responsible for sending committed entries on
// rf.applyCh. It watches newApplyReadyCh for notifications and calculates
// which new entries are ready to be sent. This method should run in a separate
// background goroutine; rf.applyCh may be buffered and will limit how fast
// the client consumes new committed entries. Returns when newApplyReadyCh is
// closed.
func (rf *Raft) applyChSender() {
	for range rf.newApplyReadyCh {
		// Find which entries we have to apply.
		rf.lockMutex()
		// savedTerm := rf.currentTerm
		savedLastApplied := rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.unlockMutex()
		rf.dLog("applyChSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  savedLastApplied + i + 1,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
	}
	rf.dLog("applyChSender done")
}

//
// Make : the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.newApplyReadyCh = make(chan struct{}, 16)
	rf.triggerAECh = make(chan struct{}, 1)
	rf.state = Follower
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.log = []LogEntry{{
		Command: byte(1),
		Term:    0,
	}}

	atomic.StoreInt32(&rf.dead, 0)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go func() {
		rf.lockMutex()
		rf.electionResetEvent = time.Now()
		rf.unlockMutex()
		rf.ticker()
	}()

	go rf.applyChSender()
	return rf
}

func (rf *Raft) unlockMutex() {
	rf.mu.Unlock()
	//rf.dLog("Mutex unlocked")
}

func (rf *Raft) lockMutex() {
	//rf.dLog("try to lock mutex")
	rf.mu.Lock()
	//rf.dLog("Mutex locked")
}
