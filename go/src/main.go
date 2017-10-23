package main

import "fmt"

func main() {
	t := NewBTree(2, true)
	t.Add([]BTreeElem{1,2,3,4,5,6,7,8,9,10}...)
	fmt.Println(t.String())
	t.Rem(3)
	fmt.Println(t.String())
}