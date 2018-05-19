package raftkv

import (
	"encoding/gob"
	"6.824/labrpc"
	"log"
	"6.824/raft"
	"sync"
	"fmt"
	"bytes"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64    // request id
	Type string // Get Put Append
	Key string
	Val string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore           map[string]string
	applyMap          map[int64]chan Op // apply channel for each request
	doneMap           map[int64]bool    // processed request, to prevent repeating
	lastIncludedIndex int
	lastIncludedTerm  int
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{args.Id, "Get", args.Key, ""}
	msg := fmt.Sprintf("[Get] me: %d, op: %+v", kv.me, op)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "not leader"
		msg += fmt.Sprintf(", reply: %+v", *reply)
		//DPrintf(msg)
		return
	}

	kv.mu.Lock()
	ch, exist := kv.applyMap[op.Id]
	if !exist {
		ch = make(chan Op)
		kv.applyMap[op.Id] = ch
	}
	kv.mu.Unlock()
	select {
	case <-ch:
		kv.mu.Lock()
		reply.Value = kv.kvStore[op.Key]
		delete(kv.applyMap, op.Id)
		kv.mu.Unlock()
		msg += fmt.Sprintf(", reply: %+v", *reply)
		//DPrintf(msg)
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Id, args.Op, args.Key, args.Value}
	_, _, isLeader := kv.rf.Start(op)
	msg := fmt.Sprintf("[PutAppend] me: %d, op: %+v", kv.me, op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "not leader"
		msg += fmt.Sprintf(", reply: %+v", *reply)
		//DPrintf(msg)
		return
	}

	kv.mu.Lock()
	ch, exist := kv.applyMap[op.Id]
	if !exist {
		ch = make(chan Op)
		kv.applyMap[op.Id] = ch
	}
	kv.mu.Unlock()
	select {
	case <-ch:
		kv.mu.Lock()
		delete(kv.applyMap, op.Id)
		kv.mu.Unlock()
		msg += fmt.Sprintf(", reply: %+v", *reply)
		DPrintf(msg)
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.applyMap = make(map[int64]chan Op)
	kv.doneMap = make(map[int64]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	return kv
}

func (kv *RaftKV) apply() {
	for {
		select {
		case msg := <-kv.applyCh:
			DPrintf("[apply] me: %d, msg: %+v", kv.me, msg)
			kv.mu.Lock()
			if msg.UseSnapshot {
				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&kv.lastIncludedIndex)
				d.Decode(&kv.lastIncludedTerm)
				d.Decode(&kv.kvStore)
				d.Decode(&kv.doneMap)
			} else {
				op := msg.Command.(Op)
				if !kv.doneMap[op.Id] {
					if op.Type == "Put" {
						kv.kvStore[op.Key] = op.Val
					} else if op.Type == "Append" {
						kv.kvStore[op.Key] += op.Val
					}
				}
				kv.doneMap[op.Id] = true
				ch, exist := kv.applyMap[op.Id]
				// a request may be sent multi times, when it is done, should broadcast by close instead of send
				if exist {
					close(ch)
				}
				kv.lastIncludedIndex = msg.Index
				kv.lastIncludedTerm = msg.Term
			}
			kv.mu.Unlock()

			if msg.RaftStateSize >= kv.maxraftstate {
				go kv.snapshot()
			}
		}
	}
}

func (kv *RaftKV) snapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	e.Encode(kv.kvStore)
	e.Encode(kv.doneMap)
	data := w.Bytes()

	kv.rf.Snapshot(data, kv.lastIncludedIndex, kv.lastIncludedTerm)
	DPrintf("[snapshot] me: %d, lastIncludedIndex: %d, lastIncludedTerm: %d", kv.me, kv.lastIncludedIndex, kv.lastIncludedTerm)
}