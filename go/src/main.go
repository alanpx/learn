package main

import (
	"fmt"
	"tree"
)
func main() {
	t := tree.NewBTree(2, true, "")
	t.Add([]tree.BTreeElem{1,2,3,4,5}...)
	fmt.Println(t.String())
	fmt.Println(t.LeafString())
}