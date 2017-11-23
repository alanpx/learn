package main

import (
    "fmt"
    "tree"
)
func main() {
    fileName := "/Users/xp/devspace/data/btree.data"
    t := tree.NewBTree(true, fileName, 0)
    //t.Add([]tree.BTreeElem{1,2,3,4,5,6,7,8,9}...)
    fmt.Println(t.String())
}
