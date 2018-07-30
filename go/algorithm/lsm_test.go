package algorithm

import "testing"

func TestSkipList(t *testing.T) {
    conf := LSMConf{}
    conf.BlockSize = 64
    conf.Dir = "/Users/xp/devspace/data/lsm/"
    conf.MemSyncSize = 128
    lsm := NewLSM(&conf)
    lsm.Add([]byte("0"), []byte("0"))
    lsm.SyncAll()
}