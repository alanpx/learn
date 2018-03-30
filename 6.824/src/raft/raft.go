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
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
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

	// persistent state on all servers
	state serverState
	currentTerm int
	votedFor int       // default -1
	log []Log

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int

	electionTimer *time.Timer
	beLeader      chan bool
    beFollower    chan bool  // becoming follower from leader
    electionDone  chan bool  // becoming follower from candidate
    electionCount int32      // a candidate become follower when the election count is 0
    done          int32      // raft instance die
}

type Log struct {
	Term int
	Content string
}

type serverState int

const (
	follower serverState = 0
	leader               = 1
	candidate            = 2
	heartBeatInterval    = 900 * time.Millisecond
	electionTimeout      = 1000 * time.Millisecond
	maxRandTime          = 500   // ms
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
    msg := fmt.Sprintf("[RequestVote] server: %+v, args: %+v, reply: %+v", struct{term, me int}{rf.currentTerm,rf.me}, *args, *reply)
    if args.Term < rf.currentTerm {
        DPrintf(msg)
        return
    }
    if args.Term > rf.currentTerm || rf.votedFor == args.CandidateId {
        if len(rf.log) == 0 || args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
            reply.VoteGranted = true
            rf.votedFor = args.CandidateId
            rf.electionTimer.Reset(getElectionTimeout())
        }
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        if rf.state != follower {
            rf.state = follower
            if rf.state == leader {
                rf.beFollower <- true
            } else if rf.state == candidate {
                rf.electionDone <- true
            }
        }
    }
    DPrintf(msg)
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
    msg := fmt.Sprintf("[sendRequestVote] server: %+v, to: %d, args: %+v", struct{term, me int}{rf.currentTerm,rf.me}, server, *args)
    DPrintf(msg)
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    msg += fmt.Sprintf(", reply: %+v, ok: %v", *reply, ok)
    DPrintf(msg)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if !ok {
        return ok
    }
    if rf.currentTerm < reply.Term {
        rf.currentTerm = reply.Term
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

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    reply.Success = false
    msg := fmt.Sprintf("[AppendEntries] server: %+v, args: %+v, reply: %+v", struct{term, me int}{rf.currentTerm,rf.me}, *args, *reply)
    if args.Term < rf.currentTerm {
        DPrintf(msg)
        return
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        if rf.state != follower {
            rf.state = follower
            if rf.state == leader {
                rf.beFollower <- true
            } else if rf.state == candidate {
                rf.electionDone <- true
            }
        }
    }
    reply.Success = true
    rf.electionTimer.Reset(getElectionTimeout())
    DPrintf(msg)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    msg := fmt.Sprintf("[sendAppendEntries] server: %+v, to: %d, args: %+v", struct{term, me int}{rf.currentTerm,rf.me}, server, *args)
    DPrintf(msg)
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    msg += fmt.Sprintf(", reply: %+v, ok: %v", *reply, ok)
    DPrintf(msg)
    rf.mu.Lock()
    defer rf.mu.Unlock()
	if !ok {
	    return ok
    }
    if rf.currentTerm < reply.Term {
        rf.currentTerm = reply.Term
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go func() {
        rf.heartBeat()
    }()

    go func() {
        rf.elect()
    }()

    rf.beLeader = make(chan bool, 1)
    rf.beFollower = make(chan bool, 1)
    rf.electionDone = make(chan bool, 1)
    rf.beFollower <- true

    return rf
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
                state := rf.state
                args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Round: round}
                rf.mu.Unlock()

                if state != leader {
                    break
                }
                for i := 0; i < len(rf.peers); i++ {
                    if i == rf.me {
                        continue
                    }
                    go func(server int, a *AppendEntriesArgs) {
                        rf.sendAppendEntries(server, a, new(AppendEntriesReply))
                    }(i, &args)
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
            if rf.electionTimer == nil {
                rf.electionTimer = time.NewTimer(getElectionTimeout())
            } else {
                rf.electionTimer.Reset(getElectionTimeout())
            }
            round := 0
        loop:
            for {
                round++
                select {
                case <-rf.electionTimer.C:
                    if atomic.LoadInt32(&rf.done) == 1 {
                        return
                    }
                    if rf.state != follower && rf.state != candidate {
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
    lastLogIndex := -1
    lastLogTerm := -1
    if len(rf.log) > 0 {
        lastLogIndex = len(rf.log) - 1
        lastLogTerm = rf.log[len(rf.log)-1].Term
    }
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

    msg := fmt.Sprintf("[doElection] server: %d, term: %d", rf.me, term)
    numGranted := 1    // including self
    numNotGranted := 0
    numTimeout := 0
loop:
    for {
        select {
        case <-rf.electionDone:
            msg += fmt.Sprintf(", cancel election because of becoming follower")
            DPrintf(msg)
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
    result := false
    rf.electionCount--
    if rf.currentTerm == term && numGranted > len(rf.peers)/2 {
        rf.state = leader
        rf.beLeader <- true
        result = true
    } else {
        if rf.electionCount == 0 {
            rf.state = follower
        }
    }
    msg += fmt.Sprintf(", result: %v, granted: %d, not granted: %d, timeout: %d, current_term: %d", result, numGranted, numNotGranted, numTimeout, rf.currentTerm)
    DPrintf(msg)
    rf.mu.Unlock()
}

func getElectionTimeout() time.Duration {
    return electionTimeout + time.Duration(rand.Intn(maxRandTime)) * time.Millisecond
}
