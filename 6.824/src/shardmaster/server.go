package shardmaster


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "encoding/gob"
import "fmt"
import "bytes"
import "log"
import "sort"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	applyMap          map[int64][]chan Config // apply channel for each request
	doneMap           map[int64]bool          // processed request, to prevent repeating
	lastIncludedIndex int
	lastIncludedTerm  int
}


type Op struct {
	// Your data here.
	Id int64    // request id
	Type string // Join Leave Move Query
	JoinArgs JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs MoveArgs
	QueryArgs QueryArgs
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Id: args.Id, Type: "Join", JoinArgs: *args}
	reply.WrongLeader, reply.Err, _ = sm.operate(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Id: args.Id, Type: "Leave", LeaveArgs: *args}
	reply.WrongLeader, reply.Err, _ = sm.operate(op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Id: args.Id, Type: "Move", MoveArgs: *args}
	reply.WrongLeader, reply.Err, _ = sm.operate(op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Id: args.Id, Type: "Query", QueryArgs: *args}
	reply.WrongLeader, reply.Err, reply.Config = sm.operate(op)
	if reply.WrongLeader {
		return
	}
}

func (sm *ShardMaster) operate(op Op) (bool, Err, Config) {
	_, _, isLeader := sm.rf.Start(op)
	msg := fmt.Sprintf("[operate] me: %d, op: %+v", sm.me, op)
	if !isLeader {
		msg += ", not leader"
		//DPrintf(msg)
		return true, "not leader", Config{}
	}

	sm.mu.Lock()
	ch := make(chan Config)
	i := 0
	_, exist := sm.applyMap[op.Id]
	if !exist {
		sm.applyMap[op.Id] = []chan Config{ch}
	} else {
		i = len(sm.applyMap[op.Id])
		sm.applyMap[op.Id] = append(sm.applyMap[op.Id], ch)
	}
	sm.mu.Unlock()

	select {
	case c := <-ch:
		sm.mu.Lock()
		sm.applyMap[op.Id] = append(sm.applyMap[op.Id][:i], sm.applyMap[op.Id][i+1:]...)
		if len(sm.applyMap[op.Id]) == 0 {
			delete(sm.applyMap, op.Id)
		}
		sm.mu.Unlock()
		msg += fmt.Sprintf(", config: %+v", c)
		DPrintf(msg)
		return false, "", c
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh, "shardmaster")

	// Your code here.
	sm.applyMap = make(map[int64][]chan Config)
	sm.doneMap = make(map[int64]bool)
	go sm.apply()

	return sm
}

func (sm *ShardMaster) apply() {
	for {
		select {
		case msg := <-sm.applyCh:
			//DPrintf("[apply] me: %d, msg: %+v", sm.me, msg)
			sm.mu.Lock()
			var ch []chan Config
			var config Config
			if msg.UseSnapshot {
				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&sm.lastIncludedIndex)
				d.Decode(&sm.lastIncludedTerm)
				d.Decode(&sm.configs)
				d.Decode(&sm.doneMap)
			} else {
				op := msg.Command.(Op)
				if !sm.doneMap[op.Id] {
					if op.Type == "Join" {
						c := sm.configs[len(sm.configs)-1].copy()
						c.Num++
						for i, g := range op.JoinArgs.Servers {
							c.Groups[i] = g
						}
						sm.balance(c)
						sm.configs = append(sm.configs, *c)
					} else if op.Type == "Leave" {
						c := sm.configs[len(sm.configs)-1].copy()
						c.Num++
						for _, g := range op.LeaveArgs.GIDs {
							delete(c.Groups, g)
						}
						sm.balance(c)
						sm.configs = append(sm.configs, *c)
					} else if op.Type == "Move" {
						c := sm.configs[len(sm.configs)-1].copy()
						c.Num++
						c.Shards[op.MoveArgs.Shard] = op.MoveArgs.GID
						sm.configs = append(sm.configs, *c)
					} else if op.Type == "Query" {
						if op.QueryArgs.Num == -1 {
							config = *sm.configs[len(sm.configs)-1].copy()
						} else if op.QueryArgs.Num < len(sm.configs) {
							config = *sm.configs[op.QueryArgs.Num].copy()
						}
					}
				}
				sm.doneMap[op.Id] = true
				ch = sm.applyMap[op.Id]
				sm.lastIncludedIndex = msg.Index
				sm.lastIncludedTerm = msg.Term
			}
			sm.mu.Unlock()

			for _, c := range ch {
				c <- config
			}
		}
	}
}

func (sm *ShardMaster) balance(conf *Config) {
	if len(conf.Groups) == 0 {
		conf.Shards = [NShards]int{}
	}

	groupLen := len(conf.Groups)
	gid := make([]int, groupLen)
	groupIndex := 0
	for id := range conf.Groups {
		gid[groupIndex] = id
		groupIndex++
	}
	// to maintain minimal transfer
	sort.Ints(gid)
	shardLen := len(conf.Shards)
	groupIndex = 0
	for i := range conf.Shards {
		if i < shardLen/groupLen*groupLen {
			conf.Shards[i] = gid[(i/(shardLen/groupLen))%groupLen]
		} else {
			conf.Shards[i] = gid[groupIndex]
			groupIndex++
		}
	}
}