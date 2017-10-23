package main

import "fmt"

func main() {
	btree := NewBTree(2)
	btree.Add([]BTreeElement{1,2,3,4,5,6,7,8,9}...)
	fmt.Println(btree.String())
	btree.Rem(1)
	fmt.Println(btree.String())
}