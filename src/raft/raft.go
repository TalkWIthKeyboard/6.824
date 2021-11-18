package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, currentTerm, isleader)
//   start agreement on a new log entry
// rf.GetState() (currentTerm, isLeader)
//   ask a Raft for its current currentTerm, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
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

type LogEntity struct {
	currentTerm int
	command     interface{}
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	electionTimeout   int
	role              string // Follower/Candidate/Leader
	receivedHeartbeat int32  // false 0/true 1

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int         // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     int         // candidateId that received vote in current term (or null if none)
	logs        []LogEntity // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int         // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int         // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int       // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int       // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.role == "Leader"
	rf.mu.Unlock()
	return int(term), isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// **************** AppendEntries RPC ****************

type AppendEntriesArgs struct {
	Term         int         // leader’s term
	LeaderId     int         // so follower can redirect clients
	Entries      []LogEntity // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIndex entry
}

func (args *AppendEntriesArgs) String() string {
	out, err := json.Marshal(args)
	if err != nil {
		log.Printf("%v", err)
	}
	return string(out)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	atomic.StoreInt32(&rf.receivedHeartbeat, 1)

	// If RPC request or response contains term T > currentTerm:
	//	set currentTerm = T, convert to follower (§5.1)
	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm (§5.1)
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.Term < rf.currentTerm || len(rf.logs) <= args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it  (§5.3)
	prevLog := rf.logs[args.PrevLogIndex]
	if prevLog.currentTerm != args.PrevLogTerm {
		rf.logs = rf.logs[0 : args.PrevLogIndex-1]
		// Append any new entries not already in the log
		for _, entry := range args.Entries {
			rf.logs = append(rf.logs, entry)
		}

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for _, entry := range args.Entries {
		rf.logs = append(rf.logs, entry)
	}

	// If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
	if rf.commitIndex < args.LeaderCommit {
		currentCommitIndex := len(rf.logs) - 1
		if args.LeaderCommit > currentCommitIndex {
			rf.commitIndex = currentCommitIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//log.Printf("Send AppendEntries, server: %v, args: %v", server, args.String())

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// **************** RequestVote RPC ****************

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate currentTerm
	CandidateId  int // candidate id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // currentTerm of candidate's last log entry
}

func (args *RequestVoteArgs) String() string {
	out, err := json.Marshal(args)
	if err != nil {
		log.Printf("%v", err)
	}
	return string(out)
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Me          int
	Term        int  // current currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Me = rf.me
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	// NOTE: candidate will send RequestVoteRPC to himself, so the rf.voteFor equals args.CandidateId is one of condition
	if rf.voteFor == -1 ||
		rf.voteFor == args.CandidateId &&
			len(rf.logs) == args.LastLogIndex &&
			rf.logs[len(rf.logs)-1].currentTerm == args.LastLogTerm {
		rf.voteFor = args.CandidateId
		reply.Me = rf.me
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	startAt := time.Now().UnixNano()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	endAt := time.Now().UnixNano()

	log.Printf("%v -> %v, time: %v ms", rf.me, server, (endAt-startAt)/1000000.0)
	return ok
}

// Start
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
// currentTerm. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.role == "Leader" && !rf.killed()

	// Your code here (2B).
	if isLeader {
		rf.logs = append(rf.logs, LogEntity{rf.currentTerm, command})
		rf.commitIndex += 1
		go rf.batchSendAppendEntries(false)
	}

	index := rf.commitIndex

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.role = "Candidate"                        // become a candidate firstly
	rf.currentTerm += 1                          // increment currentTerm
	rf.voteFor = rf.me                           // vote for self
	rf.electionTimeout = GetRandomElectionTime() // reset election timer
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = "Follower"
	rf.currentTerm = term
	rf.voteFor = -1
}

func (rf *Raft) becomeLeader() {
	log.Printf("The server %v become a new leader.", rf.me)
	rf.role = "Leader"

	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.matchIndex[i] = 0
	}

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
}

func (rf *Raft) providesAppendEntriesArgs(isHeartBeat bool, server int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logs := make([]LogEntity, 0)
	if !isHeartBeat {
		for i := rf.nextIndex[server]; i < rf.commitIndex; i++ {
			logs = append(logs, rf.logs[rf.commitIndex])
		}
	}

	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		logs,
		rf.commitIndex,
		rf.commitIndex - 1,
		rf.logs[rf.commitIndex-1].currentTerm,
	}
	return args
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		if role == "Follower" {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			time.Sleep(time.Millisecond * time.Duration(rf.electionTimeout))

			// start a new election if this peer hasn't received heartbeats
			if atomic.LoadInt32(&rf.receivedHeartbeat) == 0 {
				rf.becomeCandidate()
				rf.selfElection()
			} else {
				atomic.StoreInt32(&rf.receivedHeartbeat, 0)
			}
		}

		if role == "Leader" {
			time.Sleep(time.Millisecond * time.Duration(100))
			rf.batchSendAppendEntries(true)
		}
	}
}

func (rf *Raft) batchSendAppendEntries(isHeartBeat bool) {
	channel := make(chan AppendEntriesReply)
	for i := range rf.peers {
		go func(server int) {
			args := rf.providesAppendEntriesArgs(true, i)
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)
			channel <- reply
		}(i)
	}

	for range rf.peers {
		reply := <-channel
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.becomeFollower(reply.Term)
			break
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) selfElection() {
	log.Printf("Start to election, Me: %v, term: %v", rf.me, rf.currentTerm)
	channel := make(chan RequestVoteReply)

	for i := range rf.peers {
		go func(server int) {
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				len(rf.logs),
				rf.logs[len(rf.logs)-1].currentTerm,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			channel <- reply
		}(i)
	}

	var votes = 0
	for range rf.peers {
		select {
		case reply := <-channel:
			if reply.VoteGranted == true && rf.currentTerm == reply.Term {
				votes += 1
			}
			rf.mu.Lock()
			rf.nextIndex[reply.Me] = rf.commitIndex + 1
			rf.matchIndex[reply.Me] = rf.commitIndex
			if rf.currentTerm < reply.Term {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		case <-time.After(100 * time.Millisecond):
			log.Printf("timeout")
		}
	}
	log.Printf("Vote results, Me: %v, votes: %v, role: %v, term: %v", rf.me, votes, rf.role, rf.currentTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if votes > len(rf.peers)/2 && rf.role == "Candidate" {
		rf.becomeLeader()
	} else {
		rf.becomeFollower(rf.currentTerm)
	}
}

func GetRandomElectionTime() int {
	return rand.Intn(150) + 300
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[Me]. all the servers' peers[] arrays
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

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimeout = GetRandomElectionTime()
	rf.voteFor = -1
	rf.role = "Follower"
	rf.currentTerm = 0

	atomic.StoreInt32(&rf.receivedHeartbeat, 1)

	rf.logs = make([]LogEntity, 0)
	rf.logs = append(rf.logs, LogEntity{0, nil})
	rf.commitIndex = 1
	rf.lastApplied = 1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
