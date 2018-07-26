package algorithm

import (
    "testing"
    "os"
    "math/rand"
    "bytes"
    "strconv"
)

func TestBTree(t *testing.T) {
    conf := BtreeConf{}
    conf.FileName = "/Users/xp/devspace/data/btree.data"
    os.Remove(conf.FileName)
    conf.PageSize = 1024 * 8
    conf.MaxKeyLen = 10
    tr := NewBTree(&conf)
    const N= 1000
    data := make([]BTreeElem, 0, N)
    m := make(map[int][]byte)
    for i := 0; i < N; i++ {
        num := int(rand.Int31())
        k := []byte(strconv.Itoa(num))
        data = append(data, BTreeElem{k, k, 0})
        m[num] = k
    }
    tr.Add(data...)
    tr.SyncAll()

    tr = ParseBTree(conf.FileName, true)
    for k, v := range m {
        key := []byte(strconv.Itoa(k))
        exist, val := tr.Get(key)
        if !exist || bytes.Compare(key, val.Key) != 0 || bytes.Compare(v, val.Val) != 0 {
            t.Fatalf("expected: %s, got: %s", string(v), string(val.Val))
        }
    }

    for k, v := range m {
        key := []byte(strconv.Itoa(k))
        val := append([]byte("a"), v...)
        tr.Update(BTreeElem{key, val, 0})
    }
    tr.SyncAll()

    tr = ParseBTree(conf.FileName, true)
    for k, v := range m {
        key := []byte(strconv.Itoa(k))
        v = append([]byte("a"), v...)
        exist, val := tr.Get(key)
        if !exist || bytes.Compare(key, val.Key) != 0 || bytes.Compare(v, val.Val) != 0 {
            t.Fatalf("expected: %s, got: %s", string(v), string(val.Val))
        }
    }
}
