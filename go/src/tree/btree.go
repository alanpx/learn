package tree

import (
    "fmt"
    "strings"
    "os"
    "unsafe"
    "bytes"
    "encoding/binary"
    "io/ioutil"
)

const magicNumber uint32 = 0x42e09ad3  // to detect B tree data file

type BTree struct {
    degree    int  // minimum degree
    bplus     bool // is B+ tree
    root      *btreeNode
    maxPageNo pageNoType
    sync      bool
    fileName  string
    pageSize  uint32
}
type btreeNode struct {
    elements []BTreeElem
    children []*btreeNode
    pageNo   pageNoType
    // "prev" and "next" are used to create a linked list of leaf node of B+ tree
    prev *btreeNode
    next *btreeNode
}
type BTreeElem int32
type elementLenType uint32 // node elements length type on disk
type pageNoType uint32

func NewBTree(bplus bool, fileName string, degree int) *BTree {
    if fileName != "" {
        _, err := os.Stat(fileName)
        if err == nil {
            tr := parseBTree(fileName)
            if tr != nil {
                return tr
            }
        }
    }

    btree := BTree{}
    btree.bplus = bplus
    btree.fileName = fileName
    btree.sync = false
    btree.degree = degree
    btree.maxPageNo = 0
    if fileName != "" {
        btree.sync = true
        btree.pageSize = 32
        if degree == 0 {
            btree.degree = getDegree(btree.pageSize)
        }
    }
    btree.root = btree.newBtreeNode(nil, nil)
    return &btree
}
func (tree *BTree) newBtreeNode(elements []BTreeElem, children []*btreeNode) *btreeNode {
    tree.maxPageNo++
    return &btreeNode{elements, children, tree.maxPageNo, nil, nil}
}

func (tree *BTree) Add(elements ...BTreeElem) {
    for _, ele := range elements {
        // if the root is full, add a new root
        if len(tree.root.elements) == 2*tree.degree-1 {
            root := tree.root
            tree.root = tree.newBtreeNode(nil, []*btreeNode{root})
        }
        tree.addToNonFullNode(ele, tree.root)
    }
    tree.syncMeta()
}
func (tree *BTree) addToNonFullNode(ele BTreeElem, node *btreeNode) {
    var i int
    for i = len(node.elements) - 1; i >= 0; i-- {
        if ele >= node.elements[i] {
            break
        }
    }
    if node.isLeaf() {
        node.elements = append(node.elements[:i+1], append([]BTreeElem{ele}, node.elements[i+1:]...)...)
        tree.syncNode(node)
        return
    }
    degree := tree.degree
    // split the full child, upgrade the mid element of the full child to current node
    if len(node.children[i+1].elements) == 2*degree-1 {
        fullNode := node.children[i+1]
        midElement := fullNode.elements[degree-1]
        newNode := tree.newBtreeNode(nil, nil)
        // splitting leaf node of B+ tree
        if tree.bplus && fullNode.isLeaf() {
            // leave the mid element to the right child
            newNode.elements = make([]BTreeElem, degree)
            copy(newNode.elements, fullNode.elements[degree-1:])

            // create the linked list
            fullNode.next = newNode
            newNode.prev = fullNode
            if i >= 0 {
                node.children[i].next = fullNode
                fullNode.prev = node.children[i]
            }
            if i+2 < len(node.children) {
                newNode.next = node.children[i+2]
                node.children[i+2].prev = newNode
            }
        } else {
            newNode.elements = make([]BTreeElem, degree-1)
            copy(newNode.elements, fullNode.elements[degree:])
        }
        fullNode.elements = fullNode.elements[:degree-1]

        if !fullNode.isLeaf() {
            newNode.children = make([]*btreeNode, degree)
            copy(newNode.children, fullNode.children[degree:])
            fullNode.children = fullNode.children[:degree]
        }

        node.elements = append(node.elements[:i+1], append([]BTreeElem{midElement}, node.elements[i+1:]...)...)
        node.children = append(node.children[:i+2], append([]*btreeNode{newNode}, node.children[i+2:]...)...)

        if ele >= node.elements[i+1] {
            i++
        }

        tree.syncNode(node)
        tree.syncNode(fullNode)
        tree.syncNode(newNode)
    }
    tree.addToNonFullNode(ele, node.children[i+1])
}
func (node *btreeNode) isLeaf() bool {
    return len(node.children) == 0
}
func (tree *BTree) Rem(elements ...BTreeElem) {
    if len(tree.root.elements) == 0 {
        return
    }
    for _, ele := range elements {
        tree.remFromNode(ele, tree.root)
    }
    tree.syncMeta()
}
func (tree *BTree) remFromNode(ele BTreeElem, node *btreeNode) {
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
        tree.syncNode(node)
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
        tree.syncNode(node)
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
func (tree *BTree) remExtreme(node *btreeNode, isMax bool) BTreeElem {
    if node.isLeaf() {
        var ele BTreeElem
        if isMax {
            ele = node.elements[len(node.elements)-1]
            node.elements = node.elements[:len(node.elements)-1]
        } else {
            ele = node.elements[0]
            node.elements = node.elements[1:]
        }
        tree.syncNode(node)
        return ele
    }

    var n *btreeNode
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
func (tree *BTree) mergeChildren(node *btreeNode, i int) {
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
    tree.syncNode(node)
    tree.syncNode(node.children[i])
}
func (tree *BTree) balanceChild(node *btreeNode, from int, to int) {
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
            t.children = append([]*btreeNode{f.children[len(f.children)-1]}, t.children...)
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
    tree.syncNode(node)
    tree.syncNode(f)
    tree.syncNode(t)
}

func (tree *BTree) String() string {
    // use nil to indicate the level ending
    q := []*btreeNode{tree.root, nil}
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
func (tree *BTree) LeafString() string {
    if !tree.bplus {
        return ""
    }
    node := tree.root
    for !node.isLeaf() {
        node = node.children[0]
    }
    str := ""
    for node != nil {
        var s []string
        for _, v := range node.elements {
            s = append(s, fmt.Sprintf("%d", v))
        }
        str += strings.Join(s, ",") + " "
        node = node.next
    }
    return str
}

func getDegree(pageSize uint32) int {
    elementLenSize := uint32(unsafe.Sizeof(elementLenType(0)))
    elementSize := uint32(unsafe.Sizeof(BTreeElem(0)))
    pageNoSize := uint32(unsafe.Sizeof(pageNoType(0)))
    // 2*pageNoSize + elementLenSize + (2*degree-1)*elementSize + (2*degree)*pageNoSize <= pageSize
    degree := int((pageSize - 2*pageNoSize - elementLenSize + elementSize) / (2*pageNoSize + 2*elementSize))
    return degree
}
func (tree *BTree) syncNode(node *btreeNode) {
    // next(pageNoType) prev(pageNoType) elementLen(elementLenType) element children
    buf := new(bytes.Buffer)
    if node.prev != nil {
        binary.Write(buf, binary.LittleEndian, node.prev.pageNo)
    } else {
        binary.Write(buf, binary.LittleEndian, make([]byte, unsafe.Sizeof(pageNoType(0))))
    }
    if node.next != nil {
        binary.Write(buf, binary.LittleEndian, node.next.pageNo)
    } else {
        binary.Write(buf, binary.LittleEndian, make([]byte, unsafe.Sizeof(pageNoType(0))))
    }
    binary.Write(buf, binary.LittleEndian, elementLenType(len(node.elements)))
    for _, ele := range node.elements {
        binary.Write(buf, binary.LittleEndian, ele)
    }
    if len(node.elements) < 2*tree.degree-1 {
        zeroLen := (2*tree.degree - 1 - len(node.elements)) * int(unsafe.Sizeof(BTreeElem(0)))
        binary.Write(buf, binary.LittleEndian, make([]byte, zeroLen))
    }
    for _, child := range node.children {
        binary.Write(buf, binary.LittleEndian, child.pageNo)
    }
    if len(node.children) < 2*tree.degree {
        zeroLen := (2*tree.degree - len(node.children)) * int(unsafe.Sizeof(tree.maxPageNo))
        binary.Write(buf, binary.LittleEndian, make([]byte, zeroLen))
    }
    tree.writeAt(buf.Bytes(), int64(uint32(node.pageNo)*tree.pageSize))
}
func (tree *BTree) syncMeta() {
    // magicNumber(uint32) bplus(bool) maxPageNo(pageNoType) rootPageNo(pageNoType) pageSize(uint32)
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, magicNumber)
    binary.Write(buf, binary.LittleEndian, tree.bplus)
    binary.Write(buf, binary.LittleEndian, tree.maxPageNo)
    binary.Write(buf, binary.LittleEndian, tree.root.pageNo)
    binary.Write(buf, binary.LittleEndian, tree.pageSize)
    tree.writeAt(buf.Bytes(), 0)
}
func parseBTree(fileName string) *BTree {
    if fileName == "" {
        return nil
    }
    file, err := os.Open(fileName)
    if err != nil {
        fmt.Println(err)
        return nil
    }
    data, err := ioutil.ReadAll(file)
    if err != nil {
        fmt.Println(err)
        return nil
    }

    reader := bytes.NewReader(data)
    tr := &BTree{}
    var magic uint32
    binary.Read(reader, binary.LittleEndian, &magic)
    if magic != magicNumber {
        return nil
    }
    binary.Read(reader, binary.LittleEndian, &tr.bplus)
    binary.Read(reader, binary.LittleEndian, &tr.maxPageNo)
    var rootPageNo pageNoType
    binary.Read(reader, binary.LittleEndian, &rootPageNo)
    binary.Read(reader, binary.LittleEndian, &tr.pageSize)
    tr.degree = getDegree(tr.pageSize)
    tr.sync = true
    tr.fileName = fileName
    tr.root = tr.parseNode(data, rootPageNo)
    return tr
}
func (tree *BTree) parseNode(data []byte, pageNo pageNoType) *btreeNode {
    node := &btreeNode{}
    node.pageNo = pageNo
    reader := bytes.NewReader(data[tree.pageSize*uint32(pageNo):tree.pageSize*(uint32(pageNo)+1)])
    var prev, next pageNoType
    binary.Read(reader, binary.LittleEndian, &prev)
    binary.Read(reader, binary.LittleEndian, &next)
    if prev > 0 {
        node.prev = tree.parseNode(data, prev)
    }
    if next > 0 {
        node.next = tree.parseNode(data, next)
    }
    var eleLen elementLenType
    binary.Read(reader, binary.LittleEndian, &eleLen)
    var ele BTreeElem
    for i := 1; i <= 2*tree.degree-1; i++ {
        binary.Read(reader, binary.LittleEndian, &ele)
        if i <= int(eleLen) {
            node.elements = append(node.elements, ele)
        }
    }
    var child pageNoType
    for i := 1; i <= 2*tree.degree; i++ {
        binary.Read(reader, binary.LittleEndian, &child)
        if child == 0 { // leaf node
            break
        }
        if i <= int(eleLen)+1 {
            node.children = append(node.children, tree.parseNode(data, child))
        }
    }
    return node
}
func (tree *BTree) writeAt(data []byte, offset int64) {
    if !tree.sync {
        return
    }
    file, err := os.OpenFile(tree.fileName, os.O_WRONLY | os.O_CREATE, 0666)
    if err != nil {
        fmt.Println(err)
    }
    defer file.Close()
    _, err = file.WriteAt(data, offset)
    if err != nil {
        fmt.Println(err)
    }
}
