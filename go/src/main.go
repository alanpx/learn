package main

import (
    "fmt"
    "tree"
    "time"
)
func main() {
    begin := time.Now()
    conf := make(map[string]string)
    conf["bplus"] = "1"
    conf["fileName"] = "/Users/xp/devspace/data/btree.data"
    conf["sync"] = "1"
    t := tree.NewBTree(conf, true)
    for i := 0; i < 2; i++ {
        t.Add(tree.BTreeElem{tree.KeyType(i), []byte(fmt.Sprintf("%d", i))})
    }
    end := time.Now()
    fmt.Println(end.Sub(begin))
    fmt.Println(t.String())
    v := t.Get(tree.KeyType(1))
    //v := t.GetByCond(func(ele tree.BTreeElem) bool { return ele.Key >=10 && ele.Key < 20 })
    fmt.Println(v)
}
