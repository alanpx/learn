package main

import (
    "fmt"
    "tree"
)
func main() {
    conf := make(map[string]string)
    conf["bplus"] = "1"
    conf["fileName"] = "/Users/xp/devspace/data/btree.data"
    conf["sync"] = "1"
    t := tree.NewBTree(conf)
    //for i := 0; i < 10000; i++ {
    //    t.Add(tree.BTreeElem{tree.KeyType(i), []byte(fmt.Sprintf("%d", i))})
    //}
    fmt.Println(t.String())
    //v := t.GetByCond(func(ele tree.BTreeElem) bool { return ele.Key >=10 && ele.Key < 20 })
    //fmt.Println(v)
}
