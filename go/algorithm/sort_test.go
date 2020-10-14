package algorithm

import (
	"testing"
	"fmt"
	"encoding/json"
)

func printList(begin *Node, end *Node) {
	node := begin
	for node != nil && node != end {
		fmt.Printf("%2d ", node.val)
		node = node.next
	}
	fmt.Println()
}

func TestSort(t *testing.T) {
	var list, prev, node *Node
	n := 10
	input := []int{81,87,47,59,81,18,25,40,56,0}
	output := []int{0,18,25,40,47,56,59,81,81,87}
	for i := 0; i < n; i++ {
		node = &Node{input[i],nil}
		if prev != nil {
			prev.next = node
			prev = node
		} else {
			list = node
			prev = node
		}
	}
	list = quickSortList(list, nil)
	node = list
	re := true
	for i := 0; i < n; i++ {
		if node.val != output[i] {
			re = false
		}
		node = node.next
	}
	if !re {
		s, _ := json.Marshal(output)
		fmt.Printf("expected: %s, got: ", s)
		printList(list, nil)
		t.Fatal("got unexpected output")
	}
}