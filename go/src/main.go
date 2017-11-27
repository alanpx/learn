package main

import (
    "fmt"
    "tree"
)
func main() {
    conf := make(map[string]string)
    conf["bplus"] = "0"
    conf["fileName"] = "/Users/xp/devspace/data/btree.data"
    conf["sync"] = "0"
    t := tree.NewBTree(conf)
    for i := 0; i < 10; i++ {
        t.Add(tree.BTreeElem{tree.KeyType(i),nil})
    }
    fmt.Println(t.String())
}
