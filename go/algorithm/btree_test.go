package algorithm

import (
    "testing"
    "fmt"
    "os"
    "math/rand"
    "encoding/binary"
    "bytes"
)

func TestBTree(t *testing.T) {
    conf := BtreeConf{}
    conf.FileName = "/Users/xp/devspace/data/btree.data"
    os.Remove(conf.FileName)
    conf.PageSize = 1024 * 8
    conf.MaxKeyLen = 10
    tr := NewBTree(&conf)
    const N = 1000
    data := make([]BTreeElem, 0, N)
    m := make(map[uint32][]byte)
    for i := 0; i < N; i++ {
        k := rand.Uint32()
        buf := new(bytes.Buffer)
        binary.Write(buf, binary.LittleEndian, k)
        v := []byte(fmt.Sprintf("%d", k))
        data = append(data, BTreeElem{KeyType(buf.Bytes()), v})
        m[k] = v
    }
    tr.Add(data...)
    tr.SyncAll()

    trNew := ParseBTree(conf.FileName, true)
    for k, v := range m {
        buf := new(bytes.Buffer)
        binary.Write(buf, binary.LittleEndian, k)
        val := trNew.Get(KeyType(buf.Bytes()))
        if bytes.Compare(buf.Bytes(), val[0].Key) != 0 || bytes.Compare(v, val[0].Val) != 0 {
            t.Fatalf("expected: %s, got: %s", string(v), string(val[0].Val))
        }
    }
}
