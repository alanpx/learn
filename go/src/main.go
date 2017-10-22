package main

import "fmt"

func main() {
	btree := NewBTree(2)
	btree.Add([]BTreeElement{1,2,3,4,5,6,7,8}...)
	btree.Add(9)
	//btree.Rem([]BTreeElement{1}...)
	fmt.Println(btree.root.children[0].children[1].elements)
}