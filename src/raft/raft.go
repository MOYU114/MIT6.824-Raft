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
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil {
		// error reading persisted data
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
		rf.mu.Unlock()
		return
	}

	// Get the previous log index for this peer
	prevLogIndex := rf.nextIndex[peer] - 1

	if prevLogIndex < rf.getFirstLog().Index {
		// need to send snapshot - peer is too far behind
		// TODO: implement InstallSnapshot RPC
		rf.mu.Unlock()
		// For now, just return - snapshot functionality will be implemented in part 3D
		return
	} else {
		// send log entries
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.getLogTerm(prevLogIndex)
			if prevLogTerm == -1 {
				prevLogTerm = 0
			}
		}

		// Get entries to send (from nextIndex[peer] onwards)
		firstIndex := rf.getFirstLog().Index
		entries := make([]LogEntry, 0)
		if rf.nextIndex[peer] <= rf.getLastLog().Index {
			entries = rf.log[rf.nextIndex[peer]-firstIndex:]
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(peer, args, reply) {
			rf.handleAppendEntriesReply(peer, args, reply)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist() // Must persist term and votedFor changes
		rf.state = State.Follower
	}

	// Reset election timeout since we heard from leader
	rf.ResetElectionTimeout()
	rf.state = State.Follower
	rf.leaderId = args.LeaderId

	// (3B) Reply false if log doesn't contain an entry at prevLogIndex
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Success = false
		reply.Term = 0
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
		return
	}
	// Append new entries
	firstIndex := rf.getFirstLog().Index
	logChanged := false
	for index, entry := range args.Entries {
		entryIdx := args.PrevLogIndex + 1 + index
		if entryIdx-firstIndex >= len(rf.log) {
			// Need to append new entries beyond current log
			rf.log = reallocateEntriesArray(append(rf.log, args.Entries[index:]...))
			logChanged = true
			break
		} else if rf.getLogTerm(entryIdx) != entry.Term {
			// Remove conflicting entries and append new ones
			rf.log = reallocateEntriesArray(append(rf.log[:entryIdx-firstIndex], args.Entries[index:]...))
			logChanged = true
			break
		}
	}
	
	if logChanged {
		rf.persist() // Must persist log changes
	}

	// Update commit index based on leader's commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.advanceCommitIndex(args.LeaderCommit)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != State.Leader {
		return
	}

	// If RPC reply contains term T > currentTerm: set currentTerm = T, convert to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = State.Follower
		return
	}

	if reply.Success {
		// Update nextIndex and matchIndex for follower
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// Try to commit entries based on majority rule
		rf.tryCommit()
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
			rf.nextIndex[peer] = rf.getFirstLog().Index + 1
		}

		// Immediately retry after conflict resolution
		go rf.replicateOneRound(peer)
		// rf.replicateOneRound(peer)
	}
}

func (rf *Raft) tryCommit() {
	// Try to commit entries based on majority rule
	for i := rf.commitIndex + 1; i <= rf.getLastLog().Index; i++ {
		count := 1 // count self
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= i {
				count++
			}
		}

		// Only commit entries from current term (Figure 8)
		if count > len(rf.peers)/2 && rf.getLogTerm(i) == rf.currentTerm {
			rf.advanceCommitIndex(i)
		} else {
			break
		}
	}
}

func (rf *Raft) advanceCommitIndex(newCommitIndex int) {
	lastLogIndex := rf.getLastLog().Index
	if newCommitIndex > lastLogIndex {
		newCommitIndex = lastLogIndex
	}
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applierCond.Signal() // Signal the applier goroutine
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applierCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entry := make([]LogEntry, commitIndex-lastApplied)
		copy(entry, rf.log[lastApplied+1-firstIndex:commitIndex+1-firstIndex])

		rf.mu.Unlock()
		for _, logEntry := range entry {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			rf.lastApplied = logEntry.Index
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
				go rf.replicateOneRound(i)
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
				rf.BroadcastHeartbeat(true)
				rf.ResetHeartbeatTimeout() // reset heartbeat timeout
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

	rf.persist() // Must persist state changes immediately
	voteGrantedCnt := 1 // count of votes granted to this candidate
	
	// Use getLastLog() for consistent log index calculation
	lastLog := rf.getLastLog()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	// send RequestVote RPC to all other servers
	// for peer := 0; peer < len(rf.peers); peer++ {
	// 	if peer == rf.me { // do not send RequestVote to self
	// 		continue
	// 	}
	// 	go func(server int) { // send RequestVote RPC in a separate goroutine
	// 		reply := &RequestVoteReply{}                       // each goroutine gets its own reply
	// 		ok := rf.sendRequestVote(server, &voteArgs, reply) // send RequestVote RPC
	// 		if ok {
	// 			// Check if we need to step down due to higher term
	// 			// if RPC reply deny, and reply.Term > currentTerm, then the candidate cannot win the election
	// 			// step down to follower by themselves
	// 			//If RPC request or response contains term T > currentTerm set currentTerm = T, convert to follower (§5.1)
	// 			rf.mu.Lock()
	// 			if reply.Term > rf.currentTerm {
	// 				rf.currentTerm = reply.Term
	// 				rf.votedFor = -1
	// 				rf.state = State.Follower
	// 				rf.mu.Unlock()
	// 				voteReturnResult <- false
	// 				return
	// 			}
	// 			rf.mu.Unlock()
	// 			voteReturnResult <- reply.VoteGranted // send vote result to channel
	// 		} else {
	// 			voteReturnResult <- false // send false if RPC failed
	// 		}
	// 	}(peer)
	// }
	// // wait for votes
	// for {
	// 	result := <-voteReturnResult // get vote result from channel
	// 	voteReceivedCnt++
	// 	if result {
	// 		voteGrantedCnt++ // increment count of votes granted to this candidate
	// 	}
	// 	if voteGrantedCnt > len(rf.peers)/2 || voteReceivedCnt >= len(rf.peers) {
	// 		// if this candidate received more than half of the votes or all votes have been received
	// 		break
	// 	}
	// }

	// rf.mu.Lock()
	// if rf.state != State.Candidate {
	// 	// if this server is not a candidate anymore, do not change state to leader
	// 	rf.mu.Unlock()
	// 	return
	// }
	// rf.mu.Unlock()

	// rf.mu.Lock()
	// if voteGrantedCnt > len(rf.peers)/2 { // if this candidate received more than half of the votes
	// 	rf.state = State.Leader // change state to leader
	// 	rf.leaderId = rf.me     // set leader id to this server's id

	// 	// Reinitialize leader state (Figure 2)
	// 	lastLogIndex := rf.getLastLog().Index
	// 	for i := range rf.nextIndex {
	// 		rf.nextIndex[i] = lastLogIndex + 1
	// 		rf.matchIndex[i] = 0
	// 	}
	// }
	// rf.ResetElectionTimeout() // reset election timeout
	// rf.mu.Unlock()

	// // Send initial heartbeat if we became leader
	// rf.mu.Lock()
	// if rf.state == State.Leader {
	// 	DPrintf("server %v become leader in term %v", rf.me, rf.currentTerm)
	// 	rf.mu.Unlock()
	// 	rf.BroadcastHeartbeat(true)
	// } else {
	// 	rf.mu.Unlock()
	// }
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
						voteGrantedCnt += 1
						if voteGrantedCnt > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.state = State.Leader
							rf.leaderId = rf.me
							
							// Reinitialize leader state (Figure 2)
							lastLogIndex := rf.getLastLog().Index
							for i := range rf.nextIndex {
								rf.nextIndex[i] = lastLogIndex + 1
								rf.matchIndex[i] = 0
							}
							
							// Start heartbeat immediately and reset both timers
							rf.ResetElectionTimeout()  // Leader should not timeout
							rf.ResetHeartbeatTimeout()
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.state = State.Follower
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist() // Must persist state changes
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

	// Initialize nextIndex and matchIndex arrays
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	return rf
}
