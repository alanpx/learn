package algorithm

import (
    "testing"
    "fmt"
    "os"
    "math/rand"
    "reflect"
)

func TestBTree(t *testing.T) {
    conf := BtreeConf{}
    conf.fileName = "/Users/xp/devspace/data/btree.data"
    os.Remove(conf.fileName)
    conf.pageSize = 1024 * 8
    tr := NewBTree(&conf)
    const N = 1000
    data := make([]BTreeElem, 0, N)
    m := make(map[uint32][]byte)
    for i := 0; i < N; i++ {
        k := rand.Uint32()
        v := []byte(fmt.Sprintf("%d", k))
        data = append(data, BTreeElem{KeyType(k), v})
        m[k] = v
    }
    tr.Add(data...)
    trNew := ParseBTree(conf.fileName, true)
    for k, v := range m {
        val := trNew.Get(KeyType(k))
        if KeyType(k) != val[0].Key || !reflect.DeepEqual(v, val[0].Val) {
            t.Fatalf("expected: %s, got: %s", string(v), string(val[0].Val))
        }
    }
}
