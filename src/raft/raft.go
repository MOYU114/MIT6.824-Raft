package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term    int         // term when entry was received by leader
	Index   int         // position in the log
	Command interface{} // command for state machine; can be any type
}

var State = struct {
	Leader    string
	Follower  string
	Candidate string
}{
	Leader:    "leader",
	Follower:  "follower",
	Candidate: "candidate",
}

// A LogEntry represents a single entry in the Raft log.

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers:
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// volatile state on all servers:
	commitIndex int    // index of highest log entry known to be committed
	lastApplied int    // index of highest log entry applied to state machine
	leaderId    int    // server id of the leader; -1 if this server is not a leader
	state       string // current state of the server: "follower", "candidate", or "leader"

	// volatile state on leaders (reinitialized after election):
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	//ticker
	electionTimeout  *time.Timer // time when the next election should be started
	heartbeatTimeout *time.Timer // time when the next heartbeat should be sent

	applyCh        chan ApplyMsg // channel to send ApplyMsg to the service
	applierCond    *sync.Cond    // condition variable to signal the applier goroutine
	replicatorCond []*sync.Cond  // condition variable to signal the replicator goroutine

	lastSnapshotIndex int // last index of snapshot (real, started from the begining)
	lastSnapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm                 // get current term
	isleader = (rf.state == State.Leader) // check if this server is the leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.log)

	raftstate := w.Bytes()
	return raftstate

}

func (rf *Raft) persist() {
	data := rf.getPersistState()
	// snapshot := rf.persister.ReadSnapshot()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm                         int
		votedFor                            int
		log                                 []LogEntry
		lastSnapshotTerm, lastSnapshotIndex int
	)

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&lastSnapshotTerm) != nil || d.Decode(&lastSnapshotIndex) != nil || d.Decode(&log) != nil {
		DPrintf("rf read persist err!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.log = log
		rf.commitIndex = rf.lastSnapshotIndex
		rf.lastApplied = rf.lastSnapshotIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm || rf.state != State.Follower {
		rf.state = State.Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}
	// Always reset election timeout when receiving valid InstallSnapshot from leader
	rf.ResetElectionTimeout()

	lastIncludedTerm, lastIncludedIndex := args.LastIncludedTerm, args.LastIncludedIndex
	lastIndex := rf.getLastLog().Index
	if lastIncludedIndex > lastIndex {
		// Snapshot covers beyond our current log, replace entire log
		rf.log = make([]LogEntry, 1)
	} else {
		// Snapshot covers part of our log, trim the covered part
		installLen := lastIncludedIndex - rf.lastSnapshotIndex
		rf.log = reallocateEntriesArray(rf.log[installLen:])
	}
	rf.log[0] = LogEntry{Term: lastIncludedTerm, Index: lastIncludedIndex, Command: nil} //use dummy head as snapshot record
	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	DPrintf("{Node %v} [INSTALL_SNAPSHOT_SUCCESS] snapshot applied: snapshotIndex=%v term=%v, new logLen=%v commitIndex=%v",
		rf.me, lastIncludedIndex, lastIncludedTerm, len(rf.log), rf.commitIndex)

	rf.applierCond.Signal() //deal with it
	rf.persister.Save(rf.getPersistState(), args.Data)

	// Create ApplyMsg before releasing lock
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// Note: rf.mu.Unlock() is called by the defer statement at the top
	// Send ApplyMsg without holding the lock to avoid blocking
	go func() {
		rf.applyCh <- applyMsg
	}()

}

func (rf *Raft) sendInstallSnapshotToPeer(server int) {
	rf.mu.Lock()

	snap := rf.persister.ReadSnapshot()
	DPrintf("{Node %v} (leader) preparing InstallSnapshot to %v: lastIncludedIndex=%d term=%d snapLen=%d",
		rf.me, server, rf.lastSnapshotIndex, rf.lastSnapshotTerm, len(snap))

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	DPrintf("{Node %v} sendInstallSnapshotToPeer -> server %v: lastIncludedIndex=%v term=%v snapLen=%d",
		rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}

	// use goroutine + channel to establish timeout
	resultCh := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
		resultCh <- ok
	}()

	var ok bool
	select {
	case ok = <-resultCh:
		// RPC completed normally
		DPrintf("Success InstallSnapshot? %v\n", ok)
	case <-time.After(500 * time.Millisecond):
		DPrintf("{Node %v} InstallSnapshot to {Node %v} timed out", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != State.Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.state = State.Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.ResetElectionTimeout()
		rf.persist()
		return
	}

	if ok {
		// InstallSnapshot succeeded, update indices
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[server] {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	}

	// Signal replicator to continue with regular AppendEntries if there are more entries
	rf.replicatorCond[server].Signal()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.lastSnapshotIndex

	// Get the term of the index being snapshotted before modifying the log
	snapshotTerm := rf.getLogTerm(index)

	// Calculate how many entries to trim from the log
	trimCount := index - snapshotIndex

	if trimCount <= 0 {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.log = reallocateEntriesArray(rf.log[trimCount:])

	// Set the dummy head to represent the snapshot
	rf.log[0] = LogEntry{Term: snapshotTerm, Index: index, Command: nil}
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = snapshotTerm
	rf.persister.Save(rf.getPersistState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send
	LeaderCommit int        // leader's commitIndex
}
type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // for fast backup
	ConflictTerm  int  // for fast backup
}

func (rf *Raft) getFirstLog() LogEntry {
	if len(rf.log) == 0 {
		return LogEntry{Term: 0, Index: 0, Command: nil}
	}
	return rf.log[0]
}

func (rf *Raft) getLogTerm(index int) int {
	firstIndex := rf.getFirstLog().Index
	if index < firstIndex {
		return -1
	}
	if len(rf.log) == 0 || index-firstIndex >= len(rf.log) {
		return -1
	}
	return rf.log[index-firstIndex].Term
}

func (rf *Raft) matchLog(prevLogIndex int, prevLogTerm int) bool {
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// Special case: if prevLogIndex is 0, it means there's no previous log entry, which is always valid
	if prevLogIndex == 0 {
		return prevLogTerm == 0
	}
	return rf.getLogTerm(prevLogIndex) == prevLogTerm
}

func (rf *Raft) getLastLog() LogEntry {
	if len(rf.log) == 0 {
		return LogEntry{Term: 0, Index: 0, Command: nil}
	}
	return rf.log[len(rf.log)-1]
}

func reallocateEntriesArray(entries []LogEntry) []LogEntry {
	// Create a new slice to avoid memory leaks from the underlying array
	// This ensures that references to old log entries are properly garbage collected
	newEntries := make([]LogEntry, len(entries))
	copy(newEntries, entries)
	return newEntries
}

func (rf *Raft) replicateOneRound(peer int) {
	// follower has two ways to be replicated: log entries append or snapshot install

	rf.mu.Lock()
	if rf.state != State.Leader {
		DPrintf("{Node %v} cannot replicate to {Node %v} - not leader (state: %v)", rf.me, peer, rf.state)
		rf.mu.Unlock()
		return
	}

	// Get the previous log index for this peer
	prevLogIndex := rf.nextIndex[peer] - 1

	if prevLogIndex < rf.getFirstLog().Index && rf.lastSnapshotIndex > 0 {
		// need to send snapshot - peer is too far behind
		DPrintf("prevLogIndex: %d, getFirstLog.Index: %d\n", prevLogIndex, rf.getFirstLog().Index)
		rf.mu.Unlock()
		rf.sendInstallSnapshotToPeer(peer)
	} else {
		// prepare args for sendAppendEntries
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.getLogTerm(prevLogIndex)
		}

		// Get entries to send (from nextIndex[peer] onwards)
		firstIndex := rf.getFirstLog().Index
		entries := make([]LogEntry, 0)
		startIdx := rf.nextIndex[peer] - firstIndex
		if startIdx < len(rf.log) {
			entries = make([]LogEntry, len(rf.log[startIdx:]))
			copy(entries, rf.log[startIdx:])
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		currentTerm := rf.currentTerm // Capture term before unlocking
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		DPrintf("{Node %v} sending AppendEntries to {Node %v} in term %v", rf.me, peer, currentTerm)
		DPrintf("Arg: %v\n", args)

		if rf.sendAppendEntries(peer, args, reply) {
			DPrintf("{Node %v} AppendEntries RPC to {Node %v} succeeded", rf.me, peer)
			rf.handleAppendEntriesReply(peer, args, reply)
		} else {
			DPrintf("{Node %v} AppendEntries RPC to {Node %v} FAILED", rf.me, peer)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Reply false if term < currentTerm
	// Only change to follower if term is equal and we're not already follower
	// This prevents leaders from incorrectly stepping down on same-term AppendEntries
	if args.Term == rf.currentTerm && rf.state != State.Follower {
		rf.state = State.Follower
		rf.leaderId = args.LeaderId
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = State.Follower
	}

	// Reset election timeout since we heard from leader
	rf.ResetElectionTimeout()

	rf.leaderId = args.LeaderId

	// (3B) Reply false if log doesn't contain an entry at prevLogIndex
	// PrevLogIndex before snapshot, send snapsort
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Success = false
		reply.ConflictIndex = rf.getFirstLog().Index
		return
	}

	// prevLogIndex out of range
	if args.PrevLogIndex > rf.getLastLog().Index {
		reply.Success = false
		reply.ConflictIndex = rf.getLastLog().Index + 1
		return
	}
	// conflict resolution for fast backup (Follower)
	// If follower's log too short, tell leader to newtIndex = last log index + 1
	// If term conflict, tell leader the conflict term and the first index of that term
	if !rf.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		lastIndex := rf.getLastLog().Index
		// report conflict index for fast backup
		if lastIndex < args.PrevLogIndex { // current rf.log is shorter than prevLogIndex
			reply.ConflictIndex = lastIndex + 1
			reply.ConflictTerm = -1
		} else { //find the first index of the conflicting term, change based on leader's term and index
			firstIndex := rf.getFirstLog().Index
			reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
			index := args.PrevLogIndex - 1
			// back up to find the first index of the conflicting term
			for index >= firstIndex && rf.getLogTerm(index) == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		DPrintf("{Node %v} [APPEND_ENTRIES_CONFLICT] conflictIndex=%v, conflictTerm=%v",
			rf.me, reply.ConflictIndex, reply.ConflictTerm)
		return
	}
	// Append new entries
	firstIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
			rf.log = reallocateEntriesArray(append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...))
			break
		}
	}

	rf.advanceCommitIndex(args.LeaderCommit)
	DPrintf("{Node %v} [APPEND_ENTRIES_SUCCESS] final state: commitIndex=%v, logLen=%v",
		rf.me, rf.commitIndex, len(rf.log))

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != State.Leader || rf.currentTerm != args.Term {
		return
	}

	// If RPC reply contains term T > currentTerm: set currentTerm = T, convert to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = State.Follower
		rf.persist()
		rf.ResetElectionTimeout()
		return
	}

	if reply.Success {
		// Only update matchIndex if we actually sent entries (not just heartbeat)
		if len(args.Entries) > 0 {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			DPrintf("try commit\n")
			// Try to commit entries based on majority rule
			rf.tryCommit()
		} else {
			// For heartbeats, just confirm the nextIndex is correct
			// Don't update matchIndex since no new entries were replicated
			if rf.nextIndex[peer] <= args.PrevLogIndex+1 {
				rf.nextIndex[peer] = args.PrevLogIndex + 1
			}
		}
	} else {
		// conflict resolution for fast backup (Leader)
		if reply.ConflictTerm == -1 {
			// Follower's log is shorter than prevLogIndex
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			// Find the last entry with ConflictTerm
			conflictIndex := reply.ConflictIndex
			firstIndex := rf.getFirstLog().Index
			for i := args.PrevLogIndex; i >= firstIndex; i-- {
				if rf.getLogTerm(i) == reply.ConflictTerm {
					conflictIndex = i + 1 // Set to the index after the last matching entry
					break
				}
			}
			rf.nextIndex[peer] = conflictIndex
		}

		if rf.nextIndex[peer] < rf.getFirstLog().Index+1 {
			rf.nextIndex[peer] = rf.getFirstLog().Index
		}

		// Signal replicator to retry after conflict resolution
		rf.replicatorCond[peer].Signal()
	}
}

func (rf *Raft) tryCommit() {
	// rf.commitIndex can be ZERO due to Volatile,
	// the best way to avoid it is to ONLY COMMIT CURRENT TERM
	for i := rf.getLastLog().Index; i > rf.commitIndex; i-- {
		if rf.getLogTerm(i) != rf.currentTerm {
			continue
		}
		cnt := 1 // self
		for p := range rf.peers {
			if p != rf.me && rf.matchIndex[p] >= i {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.advanceCommitIndex(i)
			break
		}
	}
}

func (rf *Raft) advanceCommitIndex(newCommitIndex int) {
	lastLogIndex := rf.getLastLog().Index
	if newCommitIndex > lastLogIndex {
		newCommitIndex = lastLogIndex
	}
	if newCommitIndex < rf.lastSnapshotIndex {
		newCommitIndex = rf.lastSnapshotIndex
	}
	if newCommitIndex > rf.commitIndex {
		DPrintf("%v current commit: %v -> %v", rf.me, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applierCond.Signal()
	}

}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applierCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied

		entriesToApply := make([]LogEntry, 0, commitIndex-lastApplied)
		startIdx := lastApplied + 1 - firstIndex
		endIdx := commitIndex + 1 - firstIndex
		entriesToApply = append(entriesToApply, rf.log[startIdx:endIdx]...)
		rf.mu.Unlock()

		for _, logEntry := range entriesToApply {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}
			rf.applyCh <- applyMsg

			rf.mu.Lock()
			// Only update if this entry is newer than current lastApplied
			if logEntry.Index > rf.lastApplied {
				rf.lastApplied = logEntry.Index
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}

		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == State.Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm // reply with current term
	reply.VoteGranted = false   // default to not granting vote

	if args.Term < rf.currentTerm { // deny vote if term is less than current term
		return
	}

	if args.Term > rf.currentTerm { // agree
		rf.currentTerm = args.Term // update current term
		rf.votedFor = -1           // reset votedFor
		rf.persist()               // Must persist term and votedFor changes
		rf.leaderId = -1           // reset leaderId
		rf.state = State.Follower  // become follower
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // if not voted or voted for this candidate
		// need to check if candidate's log is at least as up-to-date as this server's log
		// Use getLastLog() for consistent log index calculation
		lastLog := rf.getLastLog()
		lastLogIndex := lastLog.Index
		lastLogTerm := lastLog.Term

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId // vote for candidate
			rf.persist()                   // Must persist votedFor change
			reply.VoteGranted = true       // grant vote
			rf.state = State.Follower      // change state to follower
			rf.ResetElectionTimeout()      // reset election timeout when granting vote
			DPrintf("server %v vote for %v is %v ", rf.me, args.CandidateId, reply.VoteGranted)
			return
		}
	}
}

// example code to send a RequestVote RPC to a server.
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
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC handler.

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != State.Leader {
		return -1, -1, false
	}
	newLog := rf.GetNewEntry(command)
	rf.log = append(rf.log, newLog)
	rf.persist() // Must persist log changes
	rf.BroadcastHeartbeat(false)

	return newLog.Index, newLog.Term, true
}

func (rf *Raft) GetNewEntry(command interface{}) LogEntry {
	lastLog := rf.getLastLog()
	return LogEntry{
		Term:    rf.currentTerm,
		Index:   lastLog.Index + 1,
		Command: command,
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if isHeartBeat {
				// need sending at once to maintain leadership
				// go rf.replicateOneRound(i)
				go func(peer int) {
					rf.mu.Lock()
					if rf.state != State.Leader {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					rf.replicateOneRound(peer)
				}(i)
			} else {
				// just signal replicator goroutine to send entries in batch
				rf.replicatorCond[i].Signal()
			}
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ResetElectionTimeout() {
	rf.electionTimeout.Reset(time.Duration(1000+rand.Int31n(150)) * time.Millisecond)
}

func (rf *Raft) ResetHeartbeatTimeout() {
	rf.heartbeatTimeout.Reset(time.Duration(125) * time.Millisecond)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)

		// two timeouts: one for election, one for heartbeat
		select {
		case <-rf.electionTimeout.C: // election timeout reached
			rf.mu.Lock()
			rf.StartElection()
			rf.ResetElectionTimeout()
			rf.mu.Unlock()

		case <-rf.heartbeatTimeout.C: // heartbeat timeout reached
			rf.mu.Lock()
			if rf.state == State.Leader { // only send heartbeat if this server is the leader
				DPrintf("{Node %v} Leader sending heartbeat in term %v", rf.me, rf.currentTerm)
				rf.BroadcastHeartbeat(true)
				rf.ResetHeartbeatTimeout()
			} else {
				// Non-leaders should have longer heartbeat intervals to avoid unnecessary work
				rf.heartbeatTimeout.Reset(time.Duration(500) * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) StartElection() {
	// Your code here (3A).
	// Example:
	// 1. increment current term
	// 2. vote for self
	// 3. send RequestVote RPC to all other servers
	// 4. wait for votes
	// Note: this function is called with rf.mu held
	rf.state = State.Candidate // change state to candidate
	rf.currentTerm += 1        // increment current term
	rf.votedFor = rf.me        // vote for self
	term := rf.currentTerm     // avoid data race
	rf.persist()               // Must persist state changes immediately

	// Use getLastLog() for consistent log index calculation
	lastLog := rf.getLastLog()
	voteArgs := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	// Use atomic counter for vote counting
	var grantedVotes int32 = 1 // count self vote
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, &voteArgs, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, reply, peer, voteArgs, rf.currentTerm)
				if rf.currentTerm == voteArgs.Term && rf.state == State.Candidate {
					if reply.VoteGranted {
						currentVotes := atomic.AddInt32(&grantedVotes, 1)
						if int(currentVotes) > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v, becoming Leader", rf.me, rf.currentTerm)
							rf.state = State.Leader
							rf.leaderId = rf.me

							// Reinitialize leader state (Figure 2)
							// Use very conservative approach for potentially disconnected peers
							lastLogIndex := rf.getLastLog().Index
							// firstLogIndex := rf.getFirstLog().Index
							for i := range rf.nextIndex {
								if i != rf.me {
									// Start from first log index (most conservative)
									// This will trigger snapshot installation for lagging peers
									rf.nextIndex[i] = lastLogIndex + 1
									rf.matchIndex[i] = 0
								} else {
									rf.matchIndex[i] = lastLogIndex
								}
							}

							// Start heartbeat immediately and reset both timers
							rf.ResetElectionTimeout() // Leader should not timeout
							rf.ResetHeartbeatTimeout()
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.state = State.Follower
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist() // Must persist state changes
						rf.ResetElectionTimeout()
					}
				}
			}
		}(peer)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = State.Follower // all servers start as followers
	rf.votedFor = -1          // no one has been voted for in the beginning
	rf.currentTerm = 0        // current term starts at 0
	rf.electionTimeout = time.NewTimer(time.Duration(1000+rand.Int31n(150)) * time.Millisecond)
	rf.heartbeatTimeout = time.NewTimer(time.Duration(125) * time.Millisecond)
	rf.applyCh = applyCh
	rf.applierCond = sync.NewCond(&rf.mu)
	rf.replicatorCond = make([]*sync.Cond, len(peers))

	rf.log = []LogEntry{{Term: 0, Index: 0, Command: nil}} // dummy entry at index 0, log index starts at 1

	// Initialize arrays
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Initialize nextIndex and matchIndex arrays after reading persisted state
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	for i := 0; i < len(peers); i++ {
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	return rf
}
