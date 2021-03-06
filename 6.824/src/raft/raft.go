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
    "sync"
	"6.824/labrpc"
	"time"
    "sync/atomic"
    "math/rand"
    "fmt"
    "sort"
    "bytes"
    "encoding/gob"
    "strings"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
    Term        int
    Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
    RaftStateSize int
}

func (arg ApplyMsg) String() string{
    return fmt.Sprintf("Index: %d, Term: %d, Command: %+v, UseSnapshot: %v, SnapshotLen: %d, RaftStateSize: %d", arg.Index, arg.Term, arg.Command, arg.UseSnapshot, len(arg.Snapshot), arg.RaftStateSize)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	name string
	// persistent state on all servers
	state serverState
	currentTerm int
	votedFor int       // default -1
	log []Log          // actual log starting at index 1 in absence of snapshot, otherwise starting at index 0, which is the log index lastIncludedIndex+1

	// snapshot
	hasSnapshot bool
	snapshot []byte
	isSnapshotComplete bool  // snapshot may be transfer by multi times, each time partly
    lastIncludedIndex int    // index of last log of snapshot
    lastIncludedTerm int     // term of last log of snapshot

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int    // default len(log)
	matchIndex []int

	electionTimer *time.Timer
	beLeader      chan bool
    beFollower    chan bool  // becoming follower from leader
    electionDone  chan bool  // becoming follower from candidate
    newCommit     chan bool  // commitIndex update
    electionCount int32      // a candidate become follower when the election count is 0
    done          int32      // raft instance die
}

type Log struct {
	Term int
    Command interface{}
}

type serverState int

const (
	follower serverState = 0
	leader               = 1
	candidate            = 2
	heartBeatInterval    = 40 * time.Millisecond
	electionTimeout      = 50 * time.Millisecond
	maxRandTime          = 20   // ms
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.log)
}

func (rf *Raft) persistSnapshot() {
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.snapshot)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    data := w.Bytes()
    rf.persister.SaveSnapshot(data)
}

func (rf *Raft) readPersistSnapshot(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.snapshot)
    d.Decode(&rf.lastIncludedIndex)
    d.Decode(&rf.lastIncludedTerm)
    rf.hasSnapshot = true
    rf.isSnapshotComplete = true
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
    Round int  // for debug
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    //msg := fmt.Sprintf("[RequestVote] server: %+v, args: %+v, reply: %+v", struct{term, me int}{rf.currentTerm,rf.me}, *args, *reply)
    if args.Term < rf.currentTerm {
        //rf.DPrintf(msg)
        return
    }

    if args.Term > rf.currentTerm || rf.votedFor == args.CandidateId {
        var lastLogIndex, lastLogTerm int
        done := false
        if rf.hasSnapshot && !rf.isSnapshotComplete {
            // snapshot is not complete
            done = true
        } else if !rf.hasSnapshot && len(rf.log) == 1 {
            // there is no log either in snapshot or in rf.log
            reply.VoteGranted = true
            done = true
        } else if !rf.hasSnapshot || len(rf.log) > 1 {
            // there is at least one log in rf.log
            lastLogIndex = len(rf.log)-1
            lastLogTerm = rf.log[len(rf.log)-1].Term
        } else {
            // there is no log in rf.log
            lastLogIndex = rf.lastIncludedIndex
            lastLogTerm = rf.lastIncludedTerm
        }
        if !done && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
            reply.VoteGranted = true
        }
    }
    if reply.VoteGranted {
        rf.electionTimer.Reset(getElectionTimeout())
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        if reply.VoteGranted {
            rf.votedFor = args.CandidateId
        } else {
            rf.votedFor = -1
        }
        rf.persist()
        if rf.state != follower {
            rf.state = follower
            if rf.state == leader {
                rf.beFollower <- true
            } else if rf.state == candidate {
                rf.electionDone <- true
            }
        }
    }
    //rf.DPrintf(msg)
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
    msg := fmt.Sprintf("[sendRequestVote] time: %s, server: %+v, to: %d, args: %+v", time.Now().Format("15:04:05.00"), struct{term, me int}{rf.currentTerm,rf.me}, server, *args)
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    msg += fmt.Sprintf(", reply: %+v, ok: %v", *reply, ok)
    //rf.DPrintf(msg)
    if !ok {
        return ok
    }
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.currentTerm < reply.Term {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.persist()
        if rf.state != follower {
            rf.state = follower
            if rf.state == leader {
                rf.beFollower <- true
            } else if rf.state == candidate {
                rf.electionDone <- true
            }
        }
    }
    return ok
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
	Round int  // for debug
}

func (arg AppendEntriesArgs) String() string{
    str := ""
    if len(arg.Entries) > 0 {
        str = fmt.Sprintf("%+v", arg.Entries[0])
    }
    return fmt.Sprintf("Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, EntriesLen: %d, FirstEntry: %s, LeaderCommit: %d, Round: %d", arg.Term, arg.LeaderId, arg.PrevLogIndex, arg.PrevLogTerm, len(arg.Entries), str, arg.LeaderCommit, arg.Round)
}

type AppendEntriesReply struct {
	Term int
	Success bool
    ConflictTerm int // term of the conflicting entry
    FirstIndex int   // first index of the term
    NextIndex int    // when follower's log is less than master's, use NextIndex to indicate master the next log should be sent
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    reply.Success = false
    msg := fmt.Sprintf("[AppendEntries] server: %+v, args: %+v", struct{term, me int}{rf.currentTerm,rf.me}, *args)
    if args.Term < rf.currentTerm {
        //rf.DPrintf(msg)
        return
    }

    rf.electionTimer.Reset(getElectionTimeout())
    needPersist := false
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        needPersist = true
        if rf.state != follower {
            rf.state = follower
            if rf.state == leader {
                rf.beFollower <- true
            } else if rf.state == candidate {
                rf.electionDone <- true
            }
        }
    }

    oldCommitIndex := rf.commitIndex
    if args.LeaderCommit > rf.commitIndex {
        if args.LeaderCommit > len(rf.log) - 1 {
            rf.commitIndex = len(rf.log) - 1
        } else {
            rf.commitIndex = args.LeaderCommit
        }
        rf.newCommit <- true
    }

    if len(args.Entries) == 0 {
        reply.Success = true
        if needPersist {
            rf.persist()
        }
        return
    }

    // args.Entries begin with the first log
    if args.PrevLogIndex <= 0 {
        reply.Success = true
        // should remove old entries before append
        rf.log = append(make([]Log, 1), args.Entries...)
        rf.persist()
        return
    }

    // follower's log is less than master's
    lastLogIndex := 0
    if rf.hasSnapshot {
        lastLogIndex = rf.lastIncludedIndex + len(rf.log)
    } else {
        lastLogIndex = len(rf.log) - 1
    }
    if args.PrevLogIndex > lastLogIndex {
        reply.Success = true
        reply.NextIndex = lastLogIndex + 1
        if needPersist {
            rf.persist()
        }
        return
    }

    // prevLogTerm matches
    prevLogIndex := args.PrevLogIndex
    prevLogTerm := 0
    if rf.hasSnapshot {
        if args.PrevLogIndex > rf.lastIncludedIndex {
            prevLogIndex = args.PrevLogIndex - rf.lastIncludedIndex - 1
            prevLogTerm = rf.log[prevLogIndex].Term
        } else if args.PrevLogIndex == rf.lastIncludedIndex {
            prevLogIndex = -1
            prevLogTerm = rf.lastIncludedTerm
        }
    } else {
        prevLogTerm = rf.log[args.PrevLogIndex].Term
    }
    if prevLogTerm == args.PrevLogTerm {
        reply.Success = true
        rf.log = append(rf.log[:prevLogIndex+1], args.Entries...)
        rf.persist()
        rf.DPrintf(msg + ", prevLogTerm matches")
        return
    }

    // prevLogTerm does not match, return information helping master skip logs
    reply.ConflictTerm = prevLogTerm
    i := prevLogIndex - 1
    if rf.hasSnapshot {
        for i >= 0 && rf.log[i].Term == prevLogTerm {
            i--
        }
        if i < 0 {
            i = 0
        }
        reply.FirstIndex = rf.lastIncludedIndex + i + 1
    } else {
        for i > 0 && rf.log[i].Term == prevLogTerm {
            i--
        }
        if i < 1 {
            i = 1
        }
        reply.FirstIndex = i
    }

    if needPersist {
        rf.persist()
    }
    msg += fmt.Sprintf(", reply: %+v, log: %+v, commitIndex: %d->%d, lastApplied: %d", *reply, rf.log, oldCommitIndex, rf.commitIndex, rf.lastApplied)
    rf.DPrintf(msg)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    msg := fmt.Sprintf("[sendAppendEntries] time: %s, server: %+v, to: %d, args: %+v", time.Now().Format("15:04:05.00"), struct{term, me int}{rf.currentTerm,rf.me}, server, *args)
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    msg += fmt.Sprintf(", reply: %+v, ok: %v", *reply, ok)
    //rf.DPrintf(msg)
	if !ok {
        //rf.DPrintf(msg)
	    return ok
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.currentTerm < reply.Term {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.persist()
        if rf.state != follower {
            rf.state = follower
            if rf.state == leader {
                rf.beFollower <- true
            } else if rf.state == candidate {
                rf.electionDone <- true
            }
        }
    }
    if args.Term < reply.Term {
        return ok
    }

    if len(args.Entries) == 0 {
        //rf.DPrintf(msg)
        return ok
    }

    rf.matchIndex[rf.me] = len(rf.log) - 1
    rf.nextIndex[rf.me] = len(rf.log)
    if reply.Success {
        rf.nextIndex[server] += len(args.Entries)
        rf.matchIndex[server] = rf.nextIndex[server] - 1

        // set commitIndex to the largest index matched by a majority of the servers
        tmpMatch := make([]int, len(rf.matchIndex))
        copy(tmpMatch, rf.matchIndex)
        sort.Ints(tmpMatch)
        midIndex := tmpMatch[len(tmpMatch)/2]
        midTerm := 0
        if rf.hasSnapshot {
            // the term is unknown if midIndex < rf.lastIncludedIndex
            if midIndex > rf.lastIncludedIndex {
                midTerm = rf.log[midIndex-rf.lastIncludedIndex-1].Term
            } else if midIndex == rf.lastIncludedIndex {
                midTerm = rf.lastIncludedTerm
            }
        } else {
            midTerm = rf.log[midIndex].Term
        }
        // a leader can't determine commitment using log entries from older terms. see Figure 8 from raft paper
        if midIndex > rf.commitIndex && midTerm == rf.currentTerm {
            rf.commitIndex = midIndex
            rf.newCommit <- true
        }
    } else {
        if reply.NextIndex > 0 {
            rf.nextIndex[server] = reply.NextIndex
        } else if reply.ConflictTerm > 0 {
            if rf.hasSnapshot {
                i := args.PrevLogIndex - rf.lastIncludedIndex - 1
                for i >= 0 && rf.log[i].Term != reply.ConflictTerm && i >= reply.FirstIndex-rf.lastIncludedIndex-1 {
                    i--
                }
                if i < 0 {
                    i = 0
                }
                rf.nextIndex[server] = rf.lastIncludedIndex + i + 1
            } else {
                i := args.PrevLogIndex
                for i > 0 && rf.log[i].Term != reply.ConflictTerm && i >= reply.FirstIndex {
                    i--
                }
                if i < 1 {
                    i = 1
                }
                rf.nextIndex[server] = i
            }
        } else {
            rf.nextIndex[server]--
        }
    }

    msg += fmt.Sprintf(", logLen: %d, commitIndex: %d, matchIndex: %+v, nextIndex: %+v", len(rf.log), rf.commitIndex, rf.matchIndex, rf.nextIndex)
    rf.DPrintf(msg)
    return ok
}

type InstallSnapshotArgs struct {
    Term int
    LeaderId int
    LastIncludedIndex int
    LastIncludedTerm int
    Offset int
    TotalLength int
    Data []byte
    Done bool
}

func (arg InstallSnapshotArgs) String() string{
    return fmt.Sprintf("Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedTerm: %d, Offset: %d, TotalLength: %+v, Done: %v", arg.Term, arg.LeaderId, arg.LastIncludedIndex, arg.LastIncludedTerm, arg.Offset, arg.TotalLength, arg.Done)
}

type InstallSnapshotReply struct {
    Term int
    Success bool
}

func (rf *Raft) InstallSnapshot(args * InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    msg := fmt.Sprintf("[InstallSnapshot] args: %+v", *args)
    rf.DPrintf(msg)
    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        return
    }

    rf.hasSnapshot = true
    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm
    if args.Offset == 0 {
        rf.snapshot = make([]byte, args.TotalLength)
    }
    copy(rf.snapshot[args.Offset:args.Offset+len(args.Data)], args.Data)
    rf.log = make([]Log, 0)
    if args.Done {
        rf.isSnapshotComplete = true
        rf.commitIndex = rf.lastIncludedIndex
        rf.newCommit <- true
    }

    reply.Success = true
    rf.persist()
    rf.persistSnapshot()
}

func (rf *Raft) sendInstallSnapshot(server int, args * InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    if !ok {
        return ok
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.currentTerm < reply.Term {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.persist()
        if rf.state != follower {
            rf.state = follower
            if rf.state == leader {
                rf.beFollower <- true
            } else if rf.state == candidate {
                rf.electionDone <- true
            }
        }
    }
    if args.Term < reply.Term {
        return ok
    }

    if reply.Success {
        rf.nextIndex[server] = args.LastIncludedIndex + 1
        rf.matchIndex[server] = rf.nextIndex[server] - 1
    }
    return true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
	isLeader = rf.state == leader
    if isLeader {
        index = len(rf.log)
        term = rf.currentTerm
        rf.log = append(rf.log, Log{ Term:rf.currentTerm, Command:command })
        rf.persist()
        rf.DPrintf("[Start] server: %+v, command: %+v, index: %d, hasSnapshot: %v, lastIncludedIndex: %d", struct{term, me int}{rf.currentTerm,rf.me}, command, index, rf.hasSnapshot, rf.lastIncludedIndex)
    }
    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.done, 1)
}

//
// the service or tester wants to create a Raft server. the ports
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
	persister *Persister, applyCh chan ApplyMsg, name string) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.name = name
	rf.state = follower
	rf.votedFor = -1
	rf.log = make([]Log, 1)
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    for i := 0; i < len(rf.peers); i++ {
        rf.nextIndex[i] = len(rf.log)
    }

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readPersistSnapshot(persister.ReadSnapshot())

    go rf.heartBeat()
    go rf.elect()
    go rf.apply(applyCh)

    rf.beLeader = make(chan bool, 1)
    rf.beFollower = make(chan bool, 1)
    rf.electionDone = make(chan bool, 1)
    rf.newCommit = make(chan bool, 1)
    rf.electionTimer = time.NewTimer(getElectionTimeout())
    rf.beFollower <- true

    return rf
}

func (rf *Raft) Snapshot(data []byte, lastIncludedIndex int, lastIncludedTerm int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    hasSnapshot := rf.hasSnapshot
    lastIncluded := rf.lastIncludedIndex
    rf.hasSnapshot = true
    rf.snapshot = data
    rf.isSnapshotComplete = true
    rf.lastIncludedIndex = lastIncludedIndex
    rf.lastIncludedTerm = lastIncludedTerm
    if hasSnapshot {
        rf.log = rf.log[lastIncludedIndex-lastIncluded:]
    } else {
        rf.log = rf.log[lastIncludedIndex+1:]
    }

    rf.persist()
    rf.persistSnapshot()
}

func (rf *Raft) heartBeat() {
    for {
        select {
        case <-rf.beLeader:
            round := 0
            for {
                round++
                if atomic.LoadInt32(&rf.done) == 1 {
                    return
                }

                rf.mu.Lock()
                me := rf.me
                state := rf.state
                hasSnapshot := rf.hasSnapshot
                lastIncludedIndex := rf.lastIncludedIndex
                lastIncludedTerm := rf.lastIncludedTerm
                nextIndex := make([]int, len(rf.nextIndex))
                copy(nextIndex, rf.nextIndex)
                log := make([]Log, len(rf.log))
                copy(log, rf.log)
                appendEntriesArgs := AppendEntriesArgs {
                    Term: rf.currentTerm,
                    LeaderId: rf.me,
                    LeaderCommit: rf.commitIndex,
                    Round: round,
                }
                installSnapshotArgs := InstallSnapshotArgs {
                    Term: rf.currentTerm,
                    LeaderId: rf.me,
                    LastIncludedIndex: rf.lastIncludedIndex,
                    LastIncludedTerm: rf.lastIncludedTerm,
                    Offset: 0,
                    TotalLength:len(rf.snapshot),
                    Data: rf.snapshot,
                    Done: true,
                }
                rf.mu.Unlock()

                if state != leader {
                    break
                }
                for i := 0; i < len(rf.peers); i++ {
                    if i == me {
                        continue
                    }
                    go func(server int, a AppendEntriesArgs) {
                        if hasSnapshot {
                            if nextIndex[server] <= lastIncludedIndex {
                                rf.sendInstallSnapshot(server, &installSnapshotArgs, &InstallSnapshotReply{})
                            } else if nextIndex[server] < len(log) + 1 + lastIncludedIndex {
                                a.Entries = log[nextIndex[server]-lastIncludedIndex-1:]
                                a.PrevLogIndex = nextIndex[server] - 1
                                if a.PrevLogIndex == lastIncludedIndex {
                                    a.PrevLogTerm = lastIncludedTerm
                                } else if a.PrevLogIndex > lastIncludedIndex {
                                    a.PrevLogTerm = log[a.PrevLogIndex-lastIncludedIndex-1].Term
                                }
                            }
                        } else if nextIndex[server] < len(log) {
                            a.Entries = log[nextIndex[server]:]
                            a.PrevLogIndex = nextIndex[server] - 1
                            if a.PrevLogIndex > 0 {
                                a.PrevLogTerm = log[a.PrevLogIndex].Term
                            }
                        }
                        rf.sendAppendEntries(server, &a, &AppendEntriesReply{})
                    }(i, appendEntriesArgs)
                }
                time.Sleep(heartBeatInterval)
            }
        }
    }
}

func (rf *Raft) elect() {
    for {
        select {
        case <-rf.beFollower:
            rf.electionTimer.Reset(getElectionTimeout())
            round := 0
        loop:
            for {
                round++
                select {
                case <-rf.electionTimer.C:
                    if atomic.LoadInt32(&rf.done) == 1 {
                        return
                    }
                    if rf.state == leader {
                        break loop
                    }
                    go func(r int) {
                        rf.doElection(r)
                    }(round)
                }
            }
        }
    }
}

func (rf *Raft) doElection(round int) {
    rf.mu.Lock()
    rf.state = candidate
    rf.electionCount++
    rf.currentTerm++
    term := rf.currentTerm
    rf.electionTimer.Reset(getElectionTimeout())
    rf.votedFor = rf.me
    me := rf.me
    lastLogIndex := 0
    lastLogTerm := 0
    if rf.hasSnapshot {
        lastLogIndex = rf.lastIncludedIndex + len(rf.log)
        if len(rf.log) == 0 {
            lastLogTerm = rf.lastIncludedTerm
        } else {
            lastLogTerm = rf.log[len(rf.log) - 1].Term
        }
    } else {
        lastLogIndex = len(rf.log) - 1
        lastLogTerm = rf.log[len(rf.log) - 1].Term
    }
    rf.persist()
    rf.mu.Unlock()

    var wg sync.WaitGroup
    grant := make(chan int) // -1:timeout 0:not granted 1:granted
    done := make(chan bool)
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        wg.Add(1)
        go func(server int) {
            defer wg.Done()
            args := new(RequestVoteArgs)
            args.Term = term
            args.CandidateId = me
            args.LastLogIndex = lastLogIndex
            args.LastLogTerm = lastLogTerm
            args.Round = round
            reply := new(RequestVoteReply)
            ok := rf.sendRequestVote(server, args, reply)
            re := -1
            if ok {
                if reply.VoteGranted {
                    re = 1
                } else {
                    re = 0
                }
            }
            grant <- re
        }(i)
    }
    go func() {
        wg.Wait()
        done <- true
    }()

    msg := fmt.Sprintf("[doElection] server: %d, term: %d, round: %d", rf.me, term, round)
    numGranted := 1    // including self
    numNotGranted := 0
    numTimeout := 0
loop:
    for {
        select {
        case <-rf.electionDone:
            msg += fmt.Sprintf(", cancel election because of becoming follower")
            rf.DPrintf(msg)
            rf.mu.Lock()
            rf.electionCount--
            if rf.electionCount == 0 {
                rf.state = follower
            }
            rf.mu.Unlock()
            return
        case re := <-grant:
            if re == -1 {
                numTimeout++
            } else if re == 0 {
                numNotGranted++
            } else if re == 1 {
                numGranted++
            }
            if numGranted > len(rf.peers)/2 {
                break loop
            }
        case <-done:
            break loop
        }
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    result := false
    rf.electionCount--
    if rf.currentTerm > term {
        return
    }
    if numGranted > len(rf.peers)/2 {
        rf.state = leader
        rf.beLeader <- true
        result = true
    } else {
        rf.state = follower
    }
    msg += fmt.Sprintf(", result: %v, granted: %d, not granted: %d, timeout: %d, current_term: %d, electionCount: %d", result, numGranted, numNotGranted, numTimeout, rf.currentTerm, rf.electionCount)
    rf.DPrintf(msg)
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
    for {
        select {
        case <-rf.newCommit:
            rf.mu.Lock()
            me := rf.me
            hasSnapshot := rf.hasSnapshot
            lastApplied := rf.lastApplied
            commitIndex := rf.commitIndex
            lastIncludedIndex := rf.lastIncludedIndex
            applyMsg := ApplyMsg{}
            if hasSnapshot && lastApplied < lastIncludedIndex {
                applyMsg.UseSnapshot = true
                applyMsg.Snapshot = rf.snapshot
                applyMsg.RaftStateSize = rf.persister.RaftStateSize()
                rf.lastApplied = lastIncludedIndex
            } else {
                rf.lastApplied = rf.commitIndex
            }
            rf.mu.Unlock()

            msg := fmt.Sprintf("[apply] server: %d, lastApplied: %d, commitIndex: %d, UseSnapshot: %v", me, lastApplied, commitIndex, applyMsg.UseSnapshot)
            DPrintf(msg)
            if applyMsg.UseSnapshot {
                applyCh <- applyMsg
            } else {
                for i := lastApplied+1; i <= commitIndex; i++ {
                    logIndex := i
                    if hasSnapshot {
                        logIndex = i - lastIncludedIndex - 1
                    }
                    l := rf.log[logIndex]
                    applyMsg.Term = l.Term
                    applyMsg.Command = l.Command
                    applyMsg.Index = i
                    applyMsg.RaftStateSize = rf.persister.RaftStateSize()
                    applyCh <- applyMsg
                }
            }
        }
    }
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
    f := fmt.Sprintf("[%s] %s", rf.name, format)
    ignore := []string{"shardmaster"}
    for _, s := range ignore {
        if strings.Contains(f, s) {
            return
        }
    }
    DPrintf(f, a...)
}

func getElectionTimeout() time.Duration {
    return electionTimeout + time.Duration(rand.Intn(maxRandTime)) * time.Millisecond
}
