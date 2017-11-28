package tree

import (
    "fmt"
    "strings"
    "os"
    "unsafe"
    "bytes"
    "encoding/binary"
    "io/ioutil"
    "strconv"
)

const magicNumber uint32 = 0x42e09ad3 // to detect B tree data file

type BTree struct {
    *btreeConf
    root      *btreeNode
    maxPageNo pageNoType
    dirtyNode map[pageNoType]*btreeNode
}
type btreeConf struct {
    bplus    bool // is B+ tree
    fileName string
    sync     bool
    pageSize uint32
    degree   int // minimum degree
}
type btreeNode struct {
    elements []BTreeElem
    children []*btreeNode
    pageNo   pageNoType
    // "prev" and "next" are used to create a linked list of leaf node of B+ tree
    prev *btreeNode
    next *btreeNode
    hasLoaded bool // has been loaded from disk
}
type BTreeElem struct {
    Key KeyType
    Val []byte
}
type KeyType uint32
type elementLenType uint32 // node elements length type on disk
type valueLenType uint32
type pageNoType uint32

func NewBTree(conf map[string]string, parseAll bool) *BTree {
    c := newConf(conf)
    if c.fileName != "" {
        _, err := os.Stat(c.fileName)
        if err == nil {
            tr := parseBTree(c.fileName, parseAll, c.pageSize)
            if tr != nil {
                return tr
            }
        }
    }

    t := BTree{}
    t.btreeConf = c
    t.root = t.newBtreeNode(nil, nil)
    t.dirtyNode = make(map[pageNoType]*btreeNode)
    return &t
}
func (tree *BTree) newBtreeNode(elements []BTreeElem, children []*btreeNode) *btreeNode {
    tree.maxPageNo++
    node := btreeNode{}
    node.elements = elements
    node.children = children
    node.pageNo = tree.maxPageNo
    node.hasLoaded = true
    return &node
}
func newConf(conf map[string]string) *btreeConf {
    c := btreeConf{}
    bplus, ok := conf["bplus"]
    if ok {
        c.bplus = bplus == "1"
    }
    fileName, ok := conf["fileName"]
    if ok {
        c.fileName = fileName
    }
    if c.fileName != "" {
        c.sync = true
        c.pageSize = 8*1024
    }
    sync, ok := conf["sync"]
    if ok {
        c.sync = sync == "1"
    }
    pageSize, ok := conf["pageSize"]
    if ok {
        pageSize1, _ := strconv.Atoi(pageSize)
        c.pageSize = uint32(pageSize1)
    }
    degree, ok := conf["degree"]
    if ok {
        degree1, _ := strconv.Atoi(degree)
        c.degree = degree1
    }
    if c.fileName != "" && c.degree == 0 {
        c.degree = getDegree(c.pageSize)
    }
    return &c
}
func getDegree(pageSize uint32) int {
    elementLenSize := uint32(unsafe.Sizeof(elementLenType(0)))
    keySize := uint32(unsafe.Sizeof(KeyType(0)))
    pageNoSize := uint32(unsafe.Sizeof(pageNoType(0)))
    // 2*pageNoSize + elementLenSize + (2*degree-1)*keySize + (2*degree)*pageNoSize <= pageSize
    degree := int((pageSize - 2*pageNoSize - elementLenSize + keySize) / (2*pageNoSize + 2*keySize))
    return degree
}

func (tree *BTree) Add(elements ...BTreeElem) {
    for _, ele := range elements {
        // there is no meaning for B tree to store data, just ignore it
        if !tree.bplus {
            ele.Val = nil
        }
        // if the root is full, add a new root
        if tree.isNodeFull(tree.root, nil) {
            root := tree.root
            tree.root = tree.newBtreeNode(nil, []*btreeNode{root})
        }
        tree.addToNonFullNode(ele, tree.root)
    }
    tree.syncAll()
}
func (tree *BTree) addToNonFullNode(ele BTreeElem, node *btreeNode) {
    var i int
    for i = len(node.elements) - 1; i >= 0; i-- {
        if ele.compare(node.elements[i]) >= 0 {
            break
        }
    }
    if node.isLeaf() {
        node.elements = append(node.elements[:i+1], append([]BTreeElem{ele}, node.elements[i+1:]...)...)
        tree.dirtyNode[node.pageNo] = node
        return
    }
    degree := tree.degree
    // split the full child, upgrade the mid element of the full child to current node
    if tree.isNodeFull(node.children[i+1], &ele) {
        fullNode := node.children[i+1]
        midElement := fullNode.elements[len(fullNode.elements)/2]
        // for B+ tree inner node, only keep the key
        if tree.bplus {
            midElement = BTreeElem{midElement.Key, nil}
        }
        newNode := tree.newBtreeNode(nil, nil)
        // splitting leaf node of B+ tree
        if tree.bplus && fullNode.isLeaf() {
            // leave the mid element to the right child
            newNode.elements = make([]BTreeElem, (len(fullNode.elements)+1)/2)
            copy(newNode.elements, fullNode.elements[len(fullNode.elements)/2:])

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
        fullNode.elements = fullNode.elements[:len(fullNode.elements)/2]

        if !fullNode.isLeaf() {
            newNode.children = make([]*btreeNode, degree)
            copy(newNode.children, fullNode.children[degree:])
            fullNode.children = fullNode.children[:degree]
        }

        node.elements = append(node.elements[:i+1], append([]BTreeElem{midElement}, node.elements[i+1:]...)...)
        node.children = append(node.children[:i+2], append([]*btreeNode{newNode}, node.children[i+2:]...)...)

        if ele.compare(node.elements[i+1]) >= 0 {
            i++
        }

        tree.dirtyNode[node.pageNo] = node
        tree.dirtyNode[fullNode.pageNo] = fullNode
        tree.dirtyNode[newNode.pageNo] = newNode
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
    tree.syncAll()
}
func (tree *BTree) remFromNode(ele BTreeElem, node *btreeNode) {
    var i int
    for i = len(node.elements) - 1; i >= 0; i-- {
        if ele.compare(node.elements[i]) >= 0 {
            break
        }
    }
    if node.isLeaf() {
        if i >= 0 && ele.compare(node.elements[i]) == 0 {
            node.elements = append(node.elements[:i], node.elements[i+1:]...)
        }
        tree.dirtyNode[node.pageNo] = node
        return
    }

    // remove from current node. There is no need to do this for B+ tree
    if !tree.bplus && i >= 0 && ele.compare(node.elements[i]) == 0 {
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
        tree.dirtyNode[node.pageNo] = node
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
        tree.dirtyNode[node.pageNo] = node
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
    tree.dirtyNode[node.pageNo] = node
    tree.dirtyNode[node.children[i].pageNo] = node.children[i]
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
    tree.dirtyNode[node.pageNo] = node
    tree.dirtyNode[f.pageNo] = f
    tree.dirtyNode[t.pageNo] = t
}
func (tree *BTree) Get(key KeyType) []BTreeElem {
    var re []BTreeElem
    node := tree.root
    for node != nil {
        tree.loadNode(node)
        var i int
        for i = 0; i < len(node.elements); i++ {
            if node.elements[i].compareKey(key) == 0 {
                // for b tree, return the first equal element
                if !tree.bplus {
                    re = append(re, node.elements[i])
                    return re
                } else if node.isLeaf() {
                    re = append(re, node.elements[i])
                }
            }
            if node.elements[i].compareKey(key) > 0 {
                break
            }
        }
        if node.isLeaf() {
            node = nil
        } else {
            node = node.children[i]
        }
    }
    return re
}
func (tree *BTree) GetByCond(cond func(BTreeElem) bool) []BTreeElem {
    var re []BTreeElem
    node := tree.root
    tree.loadNode(node)
    for !node.isLeaf() {
        node = node.children[0]
        tree.loadNode(node)
    }
    for node != nil {
        tree.loadNode(node)
        for i := 0; i < len(node.elements); i++ {
            if cond(node.elements[i]) {
                re = append(re, node.elements[i])
            }
        }
        node = node.next
    }
    return re
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

        str += q[0].String() + " "
        q = append(q[1:], q[0].children...)
        if q[0] == nil {
            q = append(q, nil)
        }
    }
    return str
}
func (node *btreeNode) String() string {
    var s []string
    for _, v := range node.elements {
        ele := fmt.Sprintf("%d", v.Key)
        if len(v.Val) > 0 {
            ele += fmt.Sprintf(":%d", v.Val)
        }
        s = append(s, ele)
    }
    return strings.Join(s, ",")
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

func (tree *BTree) serializeNode(node *btreeNode) (int64, []byte) {
    // common: isLeaf(bool) prev(pageNoType) next(pageNoType) elementLen(elementLenType)
    // b+ tree leaf node: element.Key element.ValLen(valueLenType) element.Val
    // else: element.Key children(pageNoType)
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, node.isLeaf())
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
        if tree.bplus && node.isLeaf() {
            binary.Write(buf, binary.LittleEndian, ele.Key)
            binary.Write(buf, binary.LittleEndian, valueLenType(len(ele.Val)))
            for _, val := range ele.Val {
                binary.Write(buf, binary.LittleEndian, val)
            }
        } else {
            binary.Write(buf, binary.LittleEndian, ele.Key)
        }
    }
    if !tree.bplus || !node.isLeaf() {
        if len(node.elements) < 2*tree.degree-1 {
            zeroLen := (2*tree.degree - 1 - len(node.elements)) * int(unsafe.Sizeof(KeyType(0)))
            binary.Write(buf, binary.LittleEndian, make([]byte, zeroLen))
        }
        for _, child := range node.children {
            binary.Write(buf, binary.LittleEndian, child.pageNo)
        }
    }
    if uint32(buf.Len()) < tree.pageSize {
        zeroLen := tree.pageSize - uint32(buf.Len())
        binary.Write(buf, binary.LittleEndian, make([]byte, zeroLen))
    }
    return int64(uint32(node.pageNo)*tree.pageSize), buf.Bytes()
}
func (tree *BTree) serializeMeta() (int64, []byte) {
    // magicNumber(uint32) bplus(bool) maxPageNo(pageNoType) rootPageNo(pageNoType) pageSize(uint32) degree(uint32)
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, magicNumber)
    binary.Write(buf, binary.LittleEndian, tree.bplus)
    binary.Write(buf, binary.LittleEndian, tree.maxPageNo)
    binary.Write(buf, binary.LittleEndian, tree.root.pageNo)
    binary.Write(buf, binary.LittleEndian, tree.pageSize)
    binary.Write(buf, binary.LittleEndian, uint32(tree.degree))
    return 0, buf.Bytes()
}
func (tree *BTree) syncAll() {
    if !tree.sync {
        return
    }
    allData := make(map[int64][]byte)
    for _, node := range tree.dirtyNode {
        offset, data := tree.serializeNode(node)
        allData[offset] = data
    }
    offset, data := tree.serializeMeta()
    allData[offset] = data
    tree.writeToDisk(allData)
}

/*
 * There are two options to avoid the cyclic reference:
 * 1. Parse children nodes recursively.
 * After the total tree has been parsed, build the leaf node linked list.
 * 2. Parse all reference nodes recursively.
 * But keep all the (partly) parsed nodes in a map.
 * We take the second option, the more general one.
 */
func parseBTree(fileName string, parseAll bool, pageSize uint32) *BTree {
    if fileName == "" {
        return nil
    }
    file, err := os.Open(fileName)
    if err != nil {
        fmt.Println(err)
        return nil
    }
    defer file.Close()
    var data []byte
    if parseAll {
        data, err = ioutil.ReadAll(file)
        if err != nil {
            fmt.Println(err)
            return nil
        }
    } else {
        data = make([]byte, pageSize)
        file.Read(data)
    }

    reader := bytes.NewReader(data)
    tr := &BTree{}
    tr.btreeConf = &btreeConf{}
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
    var degree uint32
    binary.Read(reader, binary.LittleEndian, &degree)
    tr.degree = int(degree)
    tr.sync = true
    tr.fileName = fileName
    if parseAll {
        pageNoMap := make([]*btreeNode, tr.maxPageNo+1)
        tr.root = tr.parseNode(data, rootPageNo, parseAll, pageNoMap)
    } else {
        tr.root = &btreeNode{}
        tr.root.pageNo = rootPageNo
    }
    tr.dirtyNode = make(map[pageNoType]*btreeNode)
    return tr
}
func (tree *BTree) parseNode(data []byte, pageNo pageNoType, parseAll bool, pageNoMap []*btreeNode) *btreeNode {
    if parseAll && pageNoMap[pageNo] != nil {
        return pageNoMap[pageNo]
    }

    node := &btreeNode{}
    if parseAll {
        pageNoMap[pageNo] = node
    }
    node.pageNo = pageNo
    parseData := data
    if parseAll {
        parseData = data[tree.pageSize*uint32(pageNo):tree.pageSize*(uint32(pageNo)+1)]
    }
    reader := bytes.NewReader(parseData)
    var isLeaf bool
    binary.Read(reader, binary.LittleEndian, &isLeaf)
    var prev, next pageNoType
    binary.Read(reader, binary.LittleEndian, &prev)
    binary.Read(reader, binary.LittleEndian, &next)
    if prev > 0 {
        if parseAll {
            node.prev = tree.parseNode(data, prev, parseAll, pageNoMap)
        } else {
            prevNode := btreeNode{}
            prevNode.pageNo = prev
            node.prev = &prevNode
        }
    }
    if next > 0 {
        if parseAll {
            node.next = tree.parseNode(data, next, parseAll, pageNoMap)
        } else {
            nextNode := btreeNode{}
            nextNode.pageNo = next
            node.next = &nextNode
        }
    }
    var eleLen elementLenType
    binary.Read(reader, binary.LittleEndian, &eleLen)
    ele := BTreeElem{}
    for i := 1; i <= 2*tree.degree-1; i++ {
        binary.Read(reader, binary.LittleEndian, &ele.Key)
        if tree.bplus && isLeaf {
            var valLen valueLenType
            binary.Read(reader, binary.LittleEndian, &valLen)
            var val byte
            for j := valueLenType(0); j < valLen; j++ {
                binary.Read(reader, binary.LittleEndian, &val)
                ele.Val = append(ele.Val, val)
            }
        }
        if i <= int(eleLen) {
            node.elements = append(node.elements, ele)
        }
    }
    if !isLeaf {
        var child pageNoType
        for i := elementLenType(1); i <= eleLen+1; i++ {
            binary.Read(reader, binary.LittleEndian, &child)
            if parseAll {
                node.children = append(node.children, tree.parseNode(data, child, parseAll, pageNoMap))
            } else {
                childNode := btreeNode{}
                childNode.pageNo = child
                node.children = append(node.children, &childNode)
            }
        }
    }
    return node
}
func (tree *BTree) writeToDisk(data map[int64][]byte) {
    file, err := os.OpenFile(tree.fileName, os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        fmt.Println(err)
    }
    defer file.Close()
    for offset, d := range data {
        _, err = file.WriteAt(d, offset)
        if err != nil {
            fmt.Println(err)
        }
    }
}

func (tree *BTree) isNodeFull(node *btreeNode, ele *BTreeElem) bool {
    if !tree.bplus || !node.isLeaf() {
        return len(node.elements) == 2*tree.degree-1
    }

    return tree.pageSize <= node.stringSize()+ele.stringSize()
}
func (tree *BTree) loadNode(node *btreeNode) {
    if !node.hasLoaded {
        file, err := os.Open(tree.fileName)
        if err != nil {
            fmt.Println(err)
            return
        }
        defer file.Close()
        data := make([]byte, tree.pageSize)
        file.ReadAt(data, int64(tree.pageSize)*int64(node.pageNo))
        *node = *tree.parseNode(data, node.pageNo, false, nil)
        node.hasLoaded = true
    }
}
func (node *btreeNode) isLeaf() bool {
    return len(node.children) == 0
}
func (node *btreeNode) stringSize() uint32 {
    var size uint32
    if !node.isLeaf() {
        return size
    }
    elementLenSize := uint32(unsafe.Sizeof(elementLenType(0)))
    pageNoSize := uint32(unsafe.Sizeof(pageNoType(0)))
    size += 2*pageNoSize + elementLenSize
    for _, ele := range node.elements {
        size += ele.stringSize()
    }
    return size
}
func (ele BTreeElem) compare(ele1 BTreeElem) int {
    return ele.compareKey(ele1.Key)
}
func (ele BTreeElem) compareKey(key KeyType) int {
    if ele.Key > key {
        return 1
    } else if ele.Key == key {
        return 0
    } else {
        return -1
    }
}
func (ele *BTreeElem) stringSize() uint32 {
    if ele == nil {
        return 0
    }
    return uint32(unsafe.Sizeof(ele.Key)) + uint32(unsafe.Sizeof(valueLenType(0))) + uint32(len(ele.Val))
}
