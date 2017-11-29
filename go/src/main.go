package main

import (
    "fmt"
    "time"
    "tree"
)
func main() {
    begin := time.Now()
    conf := make(map[string]string)
    conf["bplus"] = "1"
    conf["fileName"] = "/Users/xp/devspace/data/btree.data"
    conf["sync"] = "1"
    t := tree.NewBTree(conf, false)
    //const N = 1000*1000*10
    //data := make([]tree.BTreeElem,0,N)
    //for i := 0; i < N; i++ {
        //data = append(data, tree.BTreeElem{tree.KeyType(i), []byte(fmt.Sprintf("%d", i))})
    //}
    //t.Add(data...)
    //fmt.Println(t.String())
    v := t.Get(tree.KeyType(2000000))
    //v := t.GetByCond(func(ele tree.BTreeElem) bool { return ele.Key >=10 && ele.Key < 20 })
    fmt.Println(v)

    end := time.Now()
    fmt.Println(end.Sub(begin))
}
