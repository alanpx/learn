package main

import (
    "fmt"
    "tree"
)
func main() {
    fileName := "/Users/xp/devspace/data/btree.data"
    t := tree.NewBTree(true, fileName, 2)
    t.Add([]tree.BTreeElem{1,2,3,4,5}...)
    fmt.Println(t.String())
}