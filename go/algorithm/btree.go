package algorithm

import (
    "fmt"
    "strings"
    "os"
    "unsafe"
    "bytes"
    "encoding/binary"
    "io/ioutil"
    "io"
)

const (
    magicNumber  uint32 = 0x42e09ad3 // to detect B tree data file
    metaNodeSize uint32 = 24
)

type BTree struct {
    *BtreeConf
    root      *btreeNode
    maxPageNo pageNoType
    dirtyNode map[pageNoType]*btreeNode
}
type BtreeConf struct {
    FileName string
    PageSize uint32
    /*
     * minimum degree, number of elements is between [Degree-1, 2*(Degree-1)]
     * if not provided, determined by PageSize
     */
    Degree    int
    MaxKeyLen uint32
}
type btreeNode struct {
    elements []BTreeElem
    children []*btreeNode
    pageNo   pageNoType
    // "prev" and "next" are used to create a linked list of leaf node of B+ tree
    prev      *btreeNode
    next      *btreeNode
    hasLoaded bool // has been loaded from disk
}
type BTreeElem struct {
    Key KeyType
    Val ValueType
}
type KeyType []byte
type ValueType []byte
type elementLenType uint32
type keyLenType uint16
type valueLenType uint32
type pageNoType uint32

func NewBTree(conf *BtreeConf) *BTree {
    if conf.MaxKeyLen == 0 || (conf.Degree == 0 && conf.PageSize == 0) {
        return nil
    }
    if conf.PageSize > 0 && conf.Degree == 0 {
        elementLenSize := uint32(unsafe.Sizeof(elementLenType(0)))
        keySize := uint32(unsafe.Sizeof(keyLenType(0))) + uint32(conf.MaxKeyLen)
        pageNoSize := uint32(unsafe.Sizeof(pageNoType(0)))
        // 2*pageNoSize + elementLenSize + (2*Degree-1)*keySize + (2*Degree)*pageNoSize <= PageSize
        conf.Degree = int((conf.PageSize - 2*pageNoSize - elementLenSize + keySize) / (2*pageNoSize + 2*keySize))
    }
    t := BTree{}
    t.BtreeConf = conf
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

/*
 * Build tree from SORTED elements.
 * Reference: Mysql's sorted index builds.
 */
func (tree *BTree) BulkBuild(sortedElements ...BTreeElem) {
    // store the right-most node at all levels
    nodeStack := []*btreeNode{tree.root}
    var node *btreeNode
    leaf := tree.newBtreeNode(nil, nil)
    for _, ele := range sortedElements {
        if tree.nodeSize(leaf)+tree.eleSize(&ele) < tree.PageSize {
            leaf.elements = append(leaf.elements, ele)
            continue
        }
        node = nodeStack[len(nodeStack)-1]
        if len(node.children) > 0 {
            node.elements = append(node.elements, BTreeElem{leaf.elements[0].Key, nil})
        }
        node.children = append(node.children, leaf)
        tree.dirtyNode[node.pageNo] = node
        tree.dirtyNode[leaf.pageNo] = leaf
        leaf = tree.newBtreeNode(nil, nil)
        leaf.elements = append(leaf.elements, ele)
        if !tree.isNodeFull(node) {
            continue
        }
        if len(nodeStack) == 1 {
            root := tree.root
            tree.root = tree.newBtreeNode(nil, []*btreeNode{root})
            nodeStack = append([]*btreeNode{tree.root}, nodeStack...)
        }
        for i:= len(nodeStack) - 2; i >= 0; i-- {
            if tree.isNodeFull(nodeStack[i+1]) {
                newNode := tree.splitChild(nodeStack[i], len(nodeStack[i].elements))
                nodeStack[i+1] = newNode
            }
        }
    }
    if len(leaf.elements) > 0 {
        node = nodeStack[len(nodeStack)-1]
        if len(node.children) > 0 {
            node.elements = append(node.elements, BTreeElem{leaf.elements[0].Key, nil})
        }
        node.children = append(node.children, leaf)
        tree.dirtyNode[node.pageNo] = node
        tree.dirtyNode[leaf.pageNo] = leaf
    }
}
func (tree *BTree) Add(elements ...BTreeElem) {
    for _, ele := range elements {
        tree.addToNode(ele, tree.root)
    }
}
func (tree *BTree) addToNode(ele BTreeElem, node *btreeNode) {
    var i int
    for i = len(node.elements) - 1; i >= 0; i-- {
        if ele.compare(node.elements[i]) > 0 {
            break
        } else if ele.compare(node.elements[i]) == 0 {
            return
        }
    }
    if node.isLeaf() {
        node.elements = append(node.elements[:i+1], append([]BTreeElem{ele}, node.elements[i+1:]...)...)
        tree.dirtyNode[node.pageNo] = node
    } else {
        tree.addToNode(ele, node.children[i+1])
        if tree.isNodeFull(node.children[i+1]) {
            tree.splitChild(node, i+1)
        }
    }
    if node == tree.root && tree.isNodeFull(node) {
        root := tree.root
        tree.root = tree.newBtreeNode(nil, []*btreeNode{root})
        tree.splitChild(tree.root, 0)
    }
}
func (tree *BTree) Rem(elements ...BTreeElem) {
    if len(tree.root.elements) == 0 {
        return
    }
    for _, ele := range elements {
        tree.remFromNode(ele, tree.root)
    }
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

    // remove from child node
    merged := false
    if tree.isNodePartial(node.children[i+1]) {
        if i >= 0 {
            if tree.isNodePartial(node.children[i]) {
                tree.mergeChildren(node, i)
                merged = true
            } else {
                tree.balanceChild(node, i, i+1)
            }
        } else {
            if tree.isNodePartial(node.children[i+2]) {
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
        if tree.isNodePartial(n) {
            if tree.isNodePartial(node.children[len(node.children)-2]) {
                tree.mergeChildren(node, len(node.elements)-1)
            } else {
                tree.balanceChild(node, len(node.children)-2, len(node.children)-1)
            }
        }
    } else {
        n = node.children[0]
        if tree.isNodePartial(n) {
            if tree.isNodePartial(node.children[1]) {
                tree.mergeChildren(node, 0)
            } else {
                tree.balanceChild(node, 1, 0)
            }
        }
    }
    return tree.remExtreme(n, isMax)
}
func (tree *BTree) mergeChildren(node *btreeNode, i int) {
    if !node.children[i].isLeaf() {
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
        if f.isLeaf() {
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
        if f.isLeaf() {
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
                if node.isLeaf() {
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
    /* common:     isLeaf(bool) prev(pageNoType) next(pageNoType) elementLen(elementLenType)
     * leaf node:  keyLen(keyLenType) element.Key valLen(valueLenType) element.Val
     * inner node: keyLen(keyLenType) element.Key child.pageNo(pageNoType)
     */
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
        binary.Write(buf, binary.LittleEndian, keyLenType(len(ele.Key)))
        for _, key := range ele.Key {
            binary.Write(buf, binary.LittleEndian, key)
        }
        binary.Write(buf, binary.LittleEndian, make([]byte, int(tree.MaxKeyLen) - len(ele.Key)))
        if node.isLeaf() {
            binary.Write(buf, binary.LittleEndian, valueLenType(len(ele.Val)))
            for _, val := range ele.Val {
                binary.Write(buf, binary.LittleEndian, val)
            }
        }
    }
    if !node.isLeaf() {
        if len(node.elements) < 2*tree.Degree-1 {
            keySize := int(unsafe.Sizeof(keyLenType(0))) + int(tree.MaxKeyLen)
            zeroLen := (2*tree.Degree - 1 - len(node.elements)) * keySize
            binary.Write(buf, binary.LittleEndian, make([]byte, zeroLen))
        }
        for _, child := range node.children {
            binary.Write(buf, binary.LittleEndian, child.pageNo)
        }
    }
    if uint32(buf.Len()) < tree.PageSize {
        zeroLen := tree.PageSize - uint32(buf.Len())
        binary.Write(buf, binary.LittleEndian, make([]byte, zeroLen))
    }
    return int64(uint32(node.pageNo) * tree.PageSize), buf.Bytes()
}
func (tree *BTree) serializeMeta() (int64, []byte) {
    // magicNumber(uint32) maxPageNo(pageNoType) rootPageNo(pageNoType) PageSize(uint32) Degree(uint32) MaxKeyLen(uint32)
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, magicNumber)
    binary.Write(buf, binary.LittleEndian, tree.maxPageNo)
    binary.Write(buf, binary.LittleEndian, tree.root.pageNo)
    binary.Write(buf, binary.LittleEndian, tree.PageSize)
    binary.Write(buf, binary.LittleEndian, uint32(tree.Degree))
    binary.Write(buf, binary.LittleEndian, tree.MaxKeyLen)
    return 0, buf.Bytes()
}
func (tree *BTree) SyncAll() {
    if len(tree.FileName) == 0 {
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
func ParseBTree(fileName string, parseAll bool) *BTree {
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
        data = make([]byte, metaNodeSize)
        file.Read(data)
    }

    reader := bytes.NewReader(data)
    tr := &BTree{}
    tr.BtreeConf = &BtreeConf{}
    var magic uint32
    binary.Read(reader, binary.LittleEndian, &magic)
    if magic != magicNumber {
        return nil
    }
    binary.Read(reader, binary.LittleEndian, &tr.maxPageNo)
    var rootPageNo pageNoType
    binary.Read(reader, binary.LittleEndian, &rootPageNo)
    binary.Read(reader, binary.LittleEndian, &tr.PageSize)
    var degree uint32
    binary.Read(reader, binary.LittleEndian, &degree)
    binary.Read(reader, binary.LittleEndian, &tr.MaxKeyLen)
    tr.Degree = int(degree)
    tr.FileName = fileName

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
    if pageNo <= 0 || int(pageNo) >= len(pageNoMap) {
        fmt.Printf("invalid pageNo: %d\n", pageNo)
        return nil
    }
    if parseAll && pageNoMap[pageNo] != nil {
        return pageNoMap[pageNo]
    }

    node := &btreeNode{}
    node.hasLoaded = true
    if parseAll {
        pageNoMap[pageNo] = node
    }
    node.pageNo = pageNo
    parseData := data
    if parseAll {
        parseData = data[tree.PageSize*uint32(pageNo):tree.PageSize*(uint32(pageNo)+1)]
    }
    reader := bytes.NewReader(parseData)
    var isLeaf bool
    binary.Read(reader, binary.LittleEndian, &isLeaf)
    var prevPageNo, nextPageNo pageNoType
    binary.Read(reader, binary.LittleEndian, &prevPageNo)
    binary.Read(reader, binary.LittleEndian, &nextPageNo)
    if prevPageNo > 0 {
        if parseAll {
            node.prev = tree.parseNode(data, prevPageNo, parseAll, pageNoMap)
        } else {
            prevNode := btreeNode{}
            prevNode.pageNo = prevPageNo
            node.prev = &prevNode
        }
    }
    if nextPageNo > 0 {
        if parseAll {
            node.next = tree.parseNode(data, nextPageNo, parseAll, pageNoMap)
        } else {
            nextNode := btreeNode{}
            nextNode.pageNo = nextPageNo
            node.next = &nextNode
        }
    }

    var eleLen elementLenType
    binary.Read(reader, binary.LittleEndian, &eleLen)
    var ele BTreeElem
    if isLeaf {
        for i := elementLenType(0); i < eleLen; i++ {
            ele = BTreeElem{}
            var keyLen keyLenType
            binary.Read(reader, binary.LittleEndian, &keyLen)
            var key byte
            for j := keyLenType(0); j < keyLen; j++ {
                binary.Read(reader, binary.LittleEndian, &key)
                ele.Key = append(ele.Key, key)
            }
            reader.Seek(int64(tree.MaxKeyLen) - int64(keyLen), io.SeekCurrent)
            var valLen valueLenType
            binary.Read(reader, binary.LittleEndian, &valLen)
            var val byte
            for j := valueLenType(0); j < valLen; j++ {
                binary.Read(reader, binary.LittleEndian, &val)
                ele.Val = append(ele.Val, val)
            }
            node.elements = append(node.elements, ele)
        }
    } else {
        for i := elementLenType(0); i < eleLen; i++ {
            ele = BTreeElem{}
            var keyLen keyLenType
            binary.Read(reader, binary.LittleEndian, &keyLen)
            var key byte
            for j := keyLenType(0); j < keyLen; j++ {
                binary.Read(reader, binary.LittleEndian, &key)
                ele.Key = append(ele.Key, key)
            }
            reader.Seek(int64(tree.MaxKeyLen) - int64(keyLen), io.SeekCurrent)
            node.elements = append(node.elements, ele)
        }
        offset := (2*int64(tree.Degree) - 1 - int64(eleLen)) * (int64(tree.MaxKeyLen) + int64(unsafe.Sizeof(keyLenType(0))))
        reader.Seek(offset, io.SeekCurrent)
        var childPageNo pageNoType
        for i := elementLenType(0); i <= eleLen; i++ {
            binary.Read(reader, binary.LittleEndian, &childPageNo)
            if parseAll {
                node.children = append(node.children, tree.parseNode(data, childPageNo, parseAll, pageNoMap))
            } else {
                childNode := btreeNode{}
                childNode.pageNo = childPageNo
                node.children = append(node.children, &childNode)
            }
        }
    }
    return node
}
func (tree *BTree) writeToDisk(data map[int64][]byte) {
    file, err := os.OpenFile(tree.FileName, os.O_WRONLY|os.O_CREATE, 0666)
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

func (tree *BTree) isNodeFull(node *btreeNode) bool {
    full := false
    if !node.isLeaf() || tree.PageSize == 0 {
        full = len(node.elements) == 2*tree.Degree-1
    } else {
        full = tree.PageSize < tree.nodeSize(node)
    }
    return full
}
func (tree *BTree) isNodePartial(node *btreeNode) bool {
    if node.isLeaf() {
        return false
    }
    return len(node.elements) == tree.Degree- 1
}
func (tree *BTree) splitChild(node *btreeNode, childIdx int) *btreeNode {
    fullNode := node.children[childIdx]
    var midIdx int
    newNode := tree.newBtreeNode(nil, nil)
    if !fullNode.isLeaf() || tree.PageSize == 0 {
        midIdx = len(fullNode.elements)/2
        newNode.elements = make([]BTreeElem, len(fullNode.elements)-1-midIdx)
        copy(newNode.elements, fullNode.elements[midIdx+1:])
    } else {
        // leave redundant elements to the newly created node
        var excessSize uint32
        var i int
        for i = len(fullNode.elements)-1; i >= 0; i-- {
            excessSize += tree.eleSize(&fullNode.elements[i])
            if tree.nodeSize(fullNode) - excessSize <= tree.PageSize {
                break
            }
        }
        midIdx = i
        newNode.elements = make([]BTreeElem, len(fullNode.elements)-midIdx)
        copy(newNode.elements, fullNode.elements[midIdx:])

        // maintain linked list
        fullNode.next = newNode
        newNode.prev = fullNode
        if childIdx+1 < len(node.children) {
            newNode.next = node.children[childIdx+1]
            node.children[childIdx+1].prev = newNode
        }
    }

    midElement := fullNode.elements[midIdx]
    midElement.Val = nil
    fullNode.elements = fullNode.elements[:midIdx]

    if !fullNode.isLeaf() {
        newNode.children = make([]*btreeNode, len(fullNode.elements)-midIdx)
        copy(newNode.children, fullNode.children[midIdx+1:])
        fullNode.children = fullNode.children[:midIdx+1]
    }

    node.elements = append(node.elements[:childIdx], append([]BTreeElem{midElement}, node.elements[childIdx:]...)...)
    node.children = append(node.children[:childIdx+1], append([]*btreeNode{newNode}, node.children[childIdx+1:]...)...)

    tree.dirtyNode[node.pageNo] = node
    tree.dirtyNode[fullNode.pageNo] = fullNode
    tree.dirtyNode[newNode.pageNo] = newNode
    return newNode
}
func (tree *BTree) loadNode(node *btreeNode) {
    if !node.hasLoaded {
        file, err := os.Open(tree.FileName)
        if err != nil {
            fmt.Println(err)
            return
        }
        defer file.Close()
        data := make([]byte, tree.PageSize)
        file.ReadAt(data, int64(tree.PageSize)*int64(node.pageNo))
        *node = *tree.parseNode(data, node.pageNo, false, nil)
        node.hasLoaded = true
    }
}
func (node *btreeNode) isLeaf() bool {
    return len(node.children) == 0
}
func (tree *BTree) nodeSize(node *btreeNode) uint32 {
    var size uint32
    if !node.isLeaf() {
        return size
    }
    elementLenSize := uint32(unsafe.Sizeof(elementLenType(0)))
    pageNoSize := uint32(unsafe.Sizeof(pageNoType(0)))
    size += 2*pageNoSize + elementLenSize
    for _, ele := range node.elements {
        size += tree.eleSize(&ele)
    }
    return size
}
func (tree *BTree) eleSize(ele *BTreeElem) uint32 {
    if ele == nil {
        return 0
    }
    return uint32(unsafe.Sizeof(keyLenType(0))) + tree.MaxKeyLen + uint32(unsafe.Sizeof(valueLenType(0))) + uint32(len(ele.Val))
}
func (ele BTreeElem) compare(ele1 BTreeElem) int {
    return ele.compareKey(ele1.Key)
}
func (ele BTreeElem) compareKey(key KeyType) int {
    return bytes.Compare(ele.Key, key)
}
