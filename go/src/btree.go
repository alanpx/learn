package main

import (
	"fmt"
	"strings"
)

type BTree struct {
	degree int  // minimum degree
	bplus  bool // is B+ tree
	root   *BTreeNode
}
type BTreeNode struct {
	elements []BTreeElem
	children []*BTreeNode
}
type BTreeElem int

func NewBTree(degree int, bplus bool) BTree {
	return BTree{degree, bplus, &BTreeNode{}}
}
func (node *BTreeNode) isLeaf() bool {
	return len(node.children) == 0
}
func (tree *BTree) Add(elements ...BTreeElem) {
	for _, ele := range elements {
		// if the root is full, add a new root
		if len(tree.root.elements) == 2*tree.degree-1 {
			root := tree.root
			tree.root = &BTreeNode{[]BTreeElem{}, []*BTreeNode{root}}
		}
		tree.addToNonFullNode(ele, tree.root)
	}
}
func (tree *BTree) addToNonFullNode(ele BTreeElem, node *BTreeNode) {
	if node.isLeaf() {
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
		// when splitting leaf node of B+ tree, leave the mid element to the right child
		if tree.bplus && fullNode.isLeaf() {
			newNode.elements = make([]BTreeElem, degree)
			copy(newNode.elements, fullNode.elements[degree-1:])
		} else {
			newNode.elements = make([]BTreeElem, degree-1)
			copy(newNode.elements, fullNode.elements[degree:])
		}
		fullNode.elements = fullNode.elements[:degree-1]

		if !fullNode.isLeaf() {
			newNode.children = make([]*BTreeNode, degree)
			copy(newNode.children, fullNode.children[degree:])
			fullNode.children = fullNode.children[:degree]
		}

		node.elements = append(node.elements[:i+1], append([]BTreeElem{midElement}, node.elements[i+1:]...)...)
		node.children = append(node.children[:i+2], append([]*BTreeNode{&newNode}, node.children[i+2:]...)...)

		if ele >= node.elements[i+1] {
			i++
		}
	}
	tree.addToNonFullNode(ele, node.children[i+1])
}
func (tree *BTree) Rem(elements ...BTreeElem) {
	if len(tree.root.elements) == 0 {
		return
	}
	for _, ele := range elements {
		tree.remFromNode(ele, tree.root)
	}
}
func (tree *BTree) remFromNode(ele BTreeElem, node *BTreeNode) {
	var i int
	for i = len(node.elements) - 1; i >= 0; i-- {
		if ele >= node.elements[i] {
			break
		}
	}
	if node.isLeaf() {
		if i >= 0 && ele == node.elements[i] {
			node.elements = append(node.elements[:i], node.elements[i+1:]...)
		}
		return
	}

	// remove from current node. There is no need to do this for B+ tree
	if !tree.bplus && i >= 0 && ele == node.elements[i] {
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
				tree.balanceChild(node, i, i+1)
			}
		} else {
			if len(node.children[i+2].elements) == tree.degree-1 {
				tree.mergeChildren(node, i+1)
				merged = true
			} else {
				tree.balanceChild(node, i+2, i+1)
			}
		}
	}
	nodeIndex := i + 1
	if merged && i >= 0 {
		nodeIndex = i
	}
	tree.remFromNode(ele, node.children[nodeIndex])
}
func (tree *BTree) remExtreme(node *BTreeNode, isMax bool) BTreeElem {
	if node.isLeaf() {
		var ele BTreeElem
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
				tree.balanceChild(node, len(node.children)-2, len(node.children)-1)
			}
		}
	} else {
		n = node.children[0]
		if len(n.elements) == tree.degree-1 {
			if len(node.children[1].elements) == tree.degree-1 {
				tree.mergeChildren(node, 0)
			} else {
				tree.balanceChild(node, 1, 0)
			}
		}
	}
	return tree.remExtreme(n, isMax)
}
func (tree *BTree) mergeChildren(node *BTreeNode, i int) {
	if !tree.bplus || !node.children[i].isLeaf() {
		node.children[i].elements = append(node.children[i].elements, node.elements[i])
	}
	node.children[i].elements = append(node.children[i].elements, node.children[i+1].elements...)
	node.children[i].children = append(node.children[i].children, node.children[i+1].children...)
	node.elements = append(node.elements[:i], node.elements[i+1:]...)
	node.children = append(node.children[:i+1], node.children[i+2:]...)

	if node == tree.root && len(node.elements) == 0 {
		tree.root = tree.root.children[0]
	}
}
func (tree *BTree) balanceChild(node *BTreeNode, from int, to int) {
	if from-to != 1 && to-from != 1 {
		panic(fmt.Sprintf("position %d and %d are not adjacent", from, to))
	}
	f := node.children[from]
	t := node.children[to]
	if from < to {
		if tree.bplus && f.isLeaf() {
			node.elements[from] = f.elements[len(f.elements)-1]
			t.elements = append([]BTreeElem{node.elements[from]}, t.elements...)
		} else {
			t.elements = append([]BTreeElem{node.elements[from]}, t.elements...)
			node.elements[from] = f.elements[len(f.elements)-1]
		}
		f.elements = f.elements[:len(f.elements)-1]
		if !f.isLeaf() {
			t.children = append([]*BTreeNode{f.children[len(f.children)-1]}, t.children...)
			f.children = f.children[:len(f.children)-1]
		}
	} else {
		if tree.bplus && f.isLeaf() {
			node.elements[to] = f.elements[0]
			t.elements = append(t.elements, node.elements[to])
		} else {
			t.elements = append(t.elements, node.elements[to])
			node.elements[to] = f.elements[0]
		}
		f.elements = f.elements[1:]
		if !f.isLeaf() {
			t.children = append(t.children, f.children[0])
			f.children = f.children[1:]
		}
	}
}
func (tree *BTree) String() string {
	// use nil to indicate the level ending
	q := []*BTreeNode{tree.root, nil}
	str := ""
	for len(q) > 0 {
		if q[0] == nil {
			str += "\n"
			q = q[1:]
			continue
		}

		var s []string
		for _, v := range q[0].elements {
			s = append(s, fmt.Sprintf("%d", v))
		}
		str += strings.Join(s, ",") + " "
		q = append(q[1:], q[0].children...)
		if q[0] == nil {
			q = append(q, nil)
		}
	}
	return str
}
