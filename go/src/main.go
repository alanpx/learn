package main

import "fmt"

func main () {
	a := &slice{1,2}
	a.insert(3,0)
	fmt.Println(*a)
}
type BTreeElement int
type BTree struct {
	degree int      // minimum degree
	root *BTreeNode
}
type BTreeNode struct {
	elements []BTreeElement
	children []*BTreeNode
}
func (tree *BTree) add(ele BTreeElement) {
	root := tree.root
	degree := tree.degree
	// split full root
	if len(root.elements) == 2*degree-1 {
		var newNode BTreeNode
		newNode.elements = make([]BTreeElement, degree - 1)
		copy(newNode.elements, root.elements[degree:])
		newNode.children = make([]*BTreeNode, degree)
		copy(newNode.children, root.children[degree:])

		root.elements = root.elements[:degree]
		root.children = root.children[:degree]

		tree.root = &BTreeNode{[]BTreeElement{root.elements[degree-1]},[]*BTreeNode{root, &newNode}}
	}
	tree.addToNonFullNode(ele, tree.root)
}
func (tree *BTree) addToNonFullNode(ele BTreeElement, node *BTreeNode) {
	var i int
	for i = len(node.elements)-1; i>=0; i++ {
		if ele < node.elements[i] {
			i--
		}
	}
	degree := tree.degree
	// split full child
	if len(node.children[i+1].elements) == 2*degree {
		fullNode := node.children[i+1]
		var newNode BTreeNode
		newNode.elements = make([]BTreeElement, degree - 1)
		copy(newNode.elements, fullNode.elements[degree:])
		newNode.children = make([]*BTreeNode, degree)
		copy(newNode.children, fullNode.children[degree:])

		fullNode.elements = fullNode.elements[:degree]
		fullNode.children = fullNode.children[:degree]

		node.elements = append(node.elements[:i+1], append([]BTreeElement{node.children[i+1].elements[degree-1]}, node.elements[i+1:]...)...)
		node.children = append(node.children[:i+1], append([]*BTreeNode{&newNode}, node.children[i+1:]...)...)
	}
	if ele < node.elements[i+1] {
		i++
	}
	tree.addToNonFullNode(ele, node.children[i+1])
}