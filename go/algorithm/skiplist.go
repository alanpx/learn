package algorithm

import (
    "math"
    "bytes"
    "golang.org/x/exp/rand"
)

type SkipList struct {
    head     *skipListNode
    total    int // count of nodes, to determine max level
    maxLevel int // level is between [0,maxLevel)
    size     int // length of keys and values
}
type skipListNode struct {
    key  []byte
    val  []byte
    next []*skipListNode
}
type operType int

const (
    operGet operType = iota
    operAdd
    operUpdate
    operRem
)

func NewSkipList() *SkipList {
    return &SkipList{}
}
func (sl *SkipList) Get(key []byte) ([]byte, bool) {
    node, _ := sl.seek(key, operGet)
    if node == nil || bytes.Compare(key, node.key) != 0 {
        return nil, false
    }
    return node.val, true
}
func (sl *SkipList) Add(key []byte, val []byte) bool {
    return sl.add(key, val, false)
}
func (sl *SkipList) AddOrUpdate(key []byte, val []byte) {
    sl.add(key, val, true)
}
func (sl *SkipList) add(key []byte, val []byte, update bool) bool {
    sl.total++
    sl.maxLevel = int(math.Ceil(math.Log2(float64(sl.total))))
    if sl.maxLevel == 0 {
        sl.maxLevel = 1
    }
    level := rand.Intn(sl.maxLevel) + 1
    newNode := skipListNode{key, val, make([]*skipListNode, level)}
    node, prev := sl.seek(key, operAdd)
    if node != nil && bytes.Compare(key, node.key) == 0 {
        if update {
            sl.size += len(val) - len(node.val)
            node.val = val
            return true
        } else {
            return false
        }
    }

    sl.size += len(key) + len(val)
    if node == nil {
        newNode.next[0] = sl.head
        sl.head = &newNode
    }

    if node != nil && len(node.next) > 0 {
        n := node.next[0]
        level := 0
        for n != nil && level < len(n.next) && level < len(newNode.next) {
            if newNode.next[level] == nil {
                newNode.next[level] = n
            }
            if level == len(n.next)-1 {
                n = n.next[level]
            } else {
                level++
            }
        }
    }

    for i := 0; i < len(newNode.next) && i < len(prev) && prev[i] != nil; i++ {
        prev[i].next[i] = &newNode
    }
    return true
}
func (sl *SkipList) Update(key []byte, val []byte) bool {
    node, _ := sl.seek(key, operUpdate)
    if node == nil || bytes.Compare(key, node.key) != 0 {
        return false
    }

    sl.size += len(val) - len(node.val)
    node.val = val
    return true
}
func (sl *SkipList) Rem(key []byte) bool {
    sl.total--
    node, prev := sl.seek(key, operRem)
    if node == nil || bytes.Compare(key, node.key) != 0 {
        return false
    }

    sl.size -= len(node.key) + len(node.val)
    if prev == nil {
        sl.head = node.next[0]
    }

    for i := 0; i < len(node.next) && i < len(prev) && prev[i] != nil; i++ {
        prev[i].next[i] = node.next[i]
    }
    return true
}

/*
 * seek to the last node whose key is less than or equal the target key
 */
func (sl *SkipList) seek(key []byte, oper operType) (*skipListNode, []*skipListNode) {
    if sl.head == nil {
        return nil, nil
    }
    cmp := bytes.Compare(key, sl.head.key)
    if cmp < 0 {
        return nil, nil
    } else if cmp == 0 {
        return sl.head, nil
    }

    node := sl.head
    level := len(sl.head.next) - 1
    var prev []*skipListNode
    if oper == operAdd || oper == operRem {
        prev = make([]*skipListNode, sl.maxLevel)
    }
    for level >= 0 {
        if oper == operAdd || oper == operRem {
            prev[level] = node
        }
        if node.next[level] != nil {
            cmp := bytes.Compare(key, node.next[level].key)
            if cmp > 0 {
                node = node.next[level]
                level = len(node.next) - 1
            } else if cmp == 0 {
                if (oper == operGet || oper == operAdd || oper == operUpdate) || (oper == operRem && level == 0) {
                    return node.next[level], prev
                } else {
                    level--
                }
            } else {
                level--
            }
        } else {
            level--
        }
    }
    return node, prev
}
func (sl *SkipList) String() string {
    if sl.head == nil {
        return ""
    }
    buf := make([]*bytes.Buffer, sl.maxLevel)
    for i := 0; i < len(buf); i++ {
        buf[i] = new(bytes.Buffer)
    }
    node := sl.head
    for node != nil {
        for i := 0; i < sl.maxLevel; i++ {
            if i < len(node.next) {
                buf[i].Write(node.key)
                buf[i].WriteString(" ")
            } else {
                buf[i].WriteString("- ")
            }
        }
        if len(node.next) > 0 {
            node = node.next[0]
        } else {
            node = nil
        }
    }
    re := new(bytes.Buffer)
    for i := len(buf) - 1; i >= 0; i-- {
        re.WriteString(buf[i].String())
        re.WriteString("\n")
    }
    return re.String()
}
