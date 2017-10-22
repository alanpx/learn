package main

import (
	"fmt"
)

type BTree struct {
	degree int // minimum degree
	root   *BTreeNode
}
type BTreeNode struct {
	elements []BTreeElement
	children []*BTreeNode
}
type BTreeElement int

func NewBTree(degree int) BTree {
	return BTree{degree, &BTreeNode{}}
}
func (tree *BTree) Add(elements ...BTreeElement) {
	for _, ele := range elements {
		// if the root is full, add a new root
		if len(tree.root.elements) == 2*tree.degree-1 {
			root := tree.root
			tree.root = &BTreeNode{[]BTreeElement{}, []*BTreeNode{root}}
		}
		tree.addToNonFullNode(ele, tree.root)
	}
}
func (tree *BTree) addToNonFullNode(ele BTreeElement, node *BTreeNode) {
	if len(node.children) == 0 {
		node.elements = append(node.elements, ele)
		return
	}

	var i int
	for i = len(node.elements) - 1; i >= 0; i-- {
		if ele >= node.elements[i] {
			break
		}
	}
	degree := tree.degree
	// split the full child, upgrade the mid element of the full child to current node
	if len(node.children[i+1].elements) == 2*degree-1 {
		fullNode := node.children[i+1]
		midElement := fullNode.elements[degree-1]
		var newNode BTreeNode
		newNode.elements = make([]BTreeElement, degree-1)
		copy(newNode.elements, fullNode.elements[degree:])
		fullNode.elements = fullNode.elements[:degree-1]

		if len(fullNode.children) > 0 {
			newNode.children = make([]*BTreeNode, degree)
			copy(newNode.children, fullNode.children[degree:])
			fullNode.children = fullNode.children[:degree]
		}

		node.elements = append(node.elements[:i+1], append([]BTreeElement{midElement}, node.elements[i+1:]...)...)
		node.children = append(node.children[:i+2], append([]*BTreeNode{&newNode}, node.children[i+2:]...)...)

		if ele >= node.elements[i+1] {
			i++
		}
	}
	tree.addToNonFullNode(ele, node.children[i+1])
}
func (tree *BTree) Rem(elements ...BTreeElement) {
	if len(tree.root.elements) == 0 {
		return
	}
	for _, ele := range elements {
		tree.remFromNode(ele, tree.root)
	}
}
func (tree *BTree) remFromNode(ele BTreeElement, node *BTreeNode) {
	var i int
	for i = len(node.elements) - 1; i >= 0; i-- {
		if ele >= node.elements[i] {
			break
		}
	}
	if len(node.children) == 0 {
		if i >= 0 && ele == node.elements[i] {
			node.elements = append(node.elements[:i], node.elements[i+1:]...)
		}
		return
	}

	// remove from current node
	if i >= 0 && ele == node.elements[i] {
		if len(node.children[i].elements) >= tree.degree {
			del := tree.remExtreme(node.children[i], true)
			node.elements[i] = del
		} else if len(node.children[i+1].elements) >= tree.degree {
			del := tree.remExtreme(node.children[i+1], false)
			node.elements[i] = del
		} else {
			tree.mergeChildren(node, i)
			tree.remFromNode(ele, node.children[i])
		}
		return
	}

	// remove from child node
	merged := false
	if len(node.children[i+1].elements) == tree.degree-1 {
		if i >= 0 {
			if len(node.children[i].elements) == tree.degree-1 {
				tree.mergeChildren(node, i)
				merged = true
			} else {
				node.balanceChild(i, i+1)
			}
		} else {
			if len(node.children[i+2].elements) == tree.degree-1 {
				tree.mergeChildren(node, i+1)
				merged = true
			} else {
				node.balanceChild(i+2, i+1)
			}
		}
	}
	nodeIndex := i+1
	if merged && i >= 0{
		nodeIndex = i
	}
	tree.remFromNode(ele, node.children[nodeIndex])
}
func (tree *BTree) remExtreme(node *BTreeNode, isMax bool) BTreeElement {
	if len(node.children) == 0 {
		var ele BTreeElement
		if isMax {
			ele = node.elements[len(node.elements)-1]
			node.elements = node.elements[:len(node.elements)-1]
		} else {
			ele = node.elements[0]
			node.elements = node.elements[1:]
		}
		return ele
	}

	var n *BTreeNode
	if isMax {
		n = node.children[len(node.children)-1]
		if len(n.elements) == tree.degree-1 {
			if len(node.children[len(node.children)-2].elements) == tree.degree-1 {
				tree.mergeChildren(node, len(node.elements)-1)
			} else {
				node.balanceChild(len(node.children)-2, len(node.children)-1)
			}
		}
	} else {
		n = node.children[0]
		if len(n.elements) == tree.degree-1 {
			if len(node.children[1].elements) == tree.degree-1 {
				tree.mergeChildren(node, 0)
			} else {
				node.balanceChild(1, 0)
			}
		}
	}
	return tree.remExtreme(n, isMax)
}
func (tree *BTree) mergeChildren(node *BTreeNode, i int) {
	node.children[i].elements = append(node.children[i].elements, append([]BTreeElement{node.elements[i]}, node.children[i+1].elements...)...)
	node.children[i].children = append(node.children[i].children, node.children[i+1].children...)
	node.elements = append(node.elements[:i], node.elements[i+1:]...)
	node.children = append(node.children[:i+1], node.children[i+2:]...)

	if node == tree.root && len(node.elements) == 0 {
		tree.root = tree.root.children[0]
	}
}
func (node *BTreeNode) balanceChild(from int, to int) {
	if from-to != 1 && to-from != 1 {
		panic(fmt.Sprintf("position %d and %d are not adjacent", from, to))
	}
	f := node.children[from]
	t := node.children[to]
	if from < to {
		t.elements = append([]BTreeElement{node.elements[from]}, t.elements...)
		node.elements[from] = f.elements[len(f.elements)-1]
		f.elements = f.elements[:len(f.elements)-1]
		if len(f.children) > 0 {
			t.children = append([]*BTreeNode{f.children[len(f.children)-1]}, t.children...)
			f.children = f.children[:len(f.children)-1]
		}
	} else {
		t.elements = append(t.elements, node.elements[to])
		node.elements[to] = f.elements[0]
		f.elements = f.elements[1:]
		if len(f.children) > 0 {
			t.children = append(t.children, f.children[0])
			f.children = f.children[1:]
		}
	}
}