package tree

import (
    "testing"
    "time"
    "fmt"
)

func TestTree(t *testing.T) {
    begin := time.Now()
    conf := make(map[string]string)
    conf["bplus"] = "1"
    conf["fileName"] = "/Users/xp/devspace/data/btree.data"
    conf["sync"] = "1"
    //conf["pageSize"] = "64"
    //tr := tree.NewBTree(conf)
    tr := ParseBTree(conf["fileName"], false)
    const N = 1000*1000*10
    //data := make([]tree.BTreeElem, 0, N)
    //for i := 0; i < N; i++ {
    //    data = append(data, tree.BTreeElem{tree.KeyType(i), []byte(fmt.Sprintf("%d", i))})
    //}
    //tr.Add(data...)
    //tr.BulkBuild(data...)
    //fmt.Println(tr.String())
    //v := tr.Get(tree.KeyType(200001))
    v := tr.GetByCond(func(ele BTreeElem) bool { return ele.Key >=10 && ele.Key < 20 })
    fmt.Println(v)

    end := time.Now()
    fmt.Println(end.Sub(begin))
}
