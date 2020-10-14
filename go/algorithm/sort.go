package algorithm

type Node struct {
	val int
	next *Node
}
/*
 * quick sort for linked list
 * 1.the head of the list may be changed after sort
 * 2.after sort, the second half of the list should be linked to the first half
 */
func quickSortList(begin *Node, end *Node) *Node {
	if begin == nil || begin.next == nil ||
		begin == end || begin.next == end {
		return begin
	}

	head := begin
	pivotal := begin
	prev := begin
	node := begin.next
	for node != nil && node != end {
		if node.val < pivotal.val {
			prev.next = node.next
			node.next = head
			head = node
		} else {
			prev = node
		}
		node = prev.next
	}

	left := quickSortList(head, pivotal)
	right := quickSortList(pivotal.next, nil)
	pivotal.next = right
	return left
}