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
	votedFor *int
	log []Log

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int

	electionTimer *time.Timer
	leaderChan chan bool
    followerChan chan bool
}

type Log struct {
	Term int
	Content string
}

type serverState int

const (
	follower serverState = 0
	leader               = 1
	heartBeatInterval    = 900 * time.Millisecond
	electionTimeout      = 1000 * time.Millisecond
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
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    if args.Term < rf.currentTerm {
        return
    }
    if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
        if len(rf.log) == 0 || args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
            reply.VoteGranted = true
            if rf.votedFor == nil {
                rf.votedFor = new(int)
                *rf.votedFor = args.CandidateId
            }
            rf.electionTimer.Reset(getElectionTimeout())
        }
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        if rf.state != follower {
            rf.state = follower
            rf.followerChan <- true
        }
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    DPrintf("[sendRequestVote] server: %+v, to: %d, args: %+v, reply: %+v, ok: %v", struct{term, me int}{rf.currentTerm,rf.me}, server, *args, *reply, ok)
    if !ok {
	    return false
    }
    if rf.currentTerm < reply.Term {
        rf.currentTerm = reply.Term
    }
    return true
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    reply.Term = rf.currentTerm
    reply.Success = false
    if args.Term < rf.currentTerm {
        return
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        if rf.state != follower {
            rf.state = follower
            rf.followerChan <- true
        }
    }
    reply.Success = true
    rf.electionTimer.Reset(getElectionTimeout())
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    DPrintf("[sendAppendEntries] server: %+v, to: %d, args: %+v, reply: %+v, ok: %v", struct{term, me int}{rf.currentTerm,rf.me}, server, *args, *reply, ok)
    if !ok {
        return false
    }
	if rf.currentTerm < reply.Term {
	    rf.currentTerm = reply.Term
        if rf.state != follower {
            rf.state = follower
            rf.followerChan <- true
        }
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go func() {
        rf.heartBeat()
    }()

    go func() {
        rf.elect()
    }()

    rf.leaderChan = make(chan bool)
    rf.followerChan = make(chan bool)
    rf.followerChan <- true

    return rf
}

func (rf *Raft) heartBeat() {
    for {
        select {
        case <-rf.leaderChan:
            for {
                if rf.state != leader {
                    break
                }
                for i := 0; i < len(rf.peers); i++ {
                    if i == rf.me {
                        continue
                    }
                    go func(server int) {
                        args := new(AppendEntriesArgs)
                        args.Term = rf.currentTerm
                        args.LeaderId = rf.me
                        reply := new(AppendEntriesReply)
                        rf.sendAppendEntries(server, args, reply)
                    }(i)
                }
                time.Sleep(heartBeatInterval)
            }
        }
    }
}

func (rf *Raft) elect() {
    for {
        select {
        case <-rf.followerChan:
            if rf.electionTimer == nil {
                rf.electionTimer = time.NewTimer(getElectionTimeout())
            } else {
                rf.electionTimer.Reset(getElectionTimeout())
            }
        loop:
            for {
                select {
                case <-rf.electionTimer.C:
                    // Why does this if statement not work when put before select statement ?!!
                    if rf.state != follower {
                        break loop
                    }
                    go func() {
                        rf.doElection()
                    }()
                case <-rf.followerChan:
                    // Do nothing. To prevent receiving follower state concurrently.
                }
            }
        }
    }
}

func (rf *Raft) doElection() {
    rf.currentTerm++
    rf.electionTimer.Reset(getElectionTimeout())

    var wg sync.WaitGroup
    granted := new(int32)
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        wg.Add(1)
        go func(server int) {
            defer wg.Done()
            args := new(RequestVoteArgs)
            args.Term = rf.currentTerm
            args.CandidateId = rf.me
            args.LastLogIndex = -1
            args.LastLogTerm = -1
            if len(rf.log) > 0 {
                args.LastLogIndex = len(rf.log) - 1
                args.LastLogTerm = rf.log[len(rf.log)-1].Term
            }
            reply := new(RequestVoteReply)
            ok := rf.sendRequestVote(server, args, reply)
            if ok && reply.VoteGranted {
                atomic.AddInt32(granted, 1)
            }
        }(i)
    }
    wg.Wait()
    if *granted >= int32(len(rf.peers))/2 {
        rf.state = leader
        rf.leaderChan <- true
        DPrintf("[doElection] election success, server: %d, term: %d", rf.me, rf.currentTerm)
    }
}

func getElectionTimeout() time.Duration {
    return electionTimeout + time.Duration(50+rand.Intn(100)) * time.Millisecond
}
