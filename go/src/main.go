package main

import (
    "fmt"
    "tree"
)
func main() {
    t := tree.NewBTree(true, "/Users/xp/devspace/data/btree.data", 0)
    //t.Add([]tree.BTreeElem{1,2,3,4,5}...)
    fmt.Println(t.String())
}