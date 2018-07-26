package algorithm

import (
    "math"
    "bytes"
    "golang.org/x/exp/rand"
)

type SkipList struct {
    head *skipListNode
    total int      // count of nodes, to determine max level
    maxLevel int   // level is between [0,maxLevel)
}
type skipListNode struct {
    key []byte
    val []byte
    next []*skipListNode
}
func (sl *SkipList) Get(key []byte) []byte {
    node, _ := sl.seek(key, false)
    if node == nil || bytes.Compare(key, node.key) != 0 {
        return nil
    }
    return node.val
}
func (sl *SkipList) Add(key []byte, val []byte) bool {
    sl.total++
    sl.maxLevel = int(math.Log2(float64(sl.total)))
    newNode := skipListNode{key, val, make([]*skipListNode, rand.Intn(sl.maxLevel+1))}
    if sl.head == nil {
        sl.head = &newNode
        return true
    }
    cmp := bytes.Compare(key, sl.head.key)
    if cmp < 0 {
        newNode.next[0] = sl.head
        sl.head = &newNode
    } else if cmp == 0 {
        return false
    }

    node := sl.head
    level := len(sl.head.next) - 1
    prev := make([]*skipListNode, len(newNode.next))
    for level >= 0 {
        if level < len(newNode.next) {
            prev[level] = node
        }
        cmp := bytes.Compare(key, node.next[level].key)
        if cmp > 0 {
            node = node.next[level]
            level = len(node.next) - 1
        } else if cmp == 0 {
            return false
        } else {
            level--
        }
    }
    newNode.next[0] = node

    for i := 0 ; i < len(newNode.next); i++ {
        prev[i].next[i] = &newNode
    }

    node = newNode.next[0]
    level = 0
    for level < len(node.next) {
        if newNode.next[level] == nil {
            newNode.next[level] = node
        }
        if level == len(node.next) - 1 {
            node = node.next[level]
        }
    }

    return true
}
func (sl *SkipList) Rem(key []byte) bool {
    if sl == nil || sl.head == nil {
        return false
    }
    sl.total--
    cmp := bytes.Compare(key, sl.head.key)
    if cmp < 0 {
        return false
    } else if cmp == 0 {
        sl.head = sl.head.next[0]
        return true
    }

    node := sl.head
    level := len(sl.head.next) - 1
    prev := make([]*skipListNode, sl.maxLevel)
    for level >= 0 {
        if level < len(prev) {
            prev[level] = node
        }
        cmp := bytes.Compare(key, node.next[level].key)
        if cmp > 0 {
            node = node.next[level]
            level = len(node.next) - 1
        } else if cmp == 0 {
            return false
        } else {
            level--
        }
    }

    for level := 0; level < len(node.next); level++ {
        prev[level].next[level] = node.next[level]
    }
    return true
}
/*
 * seek to the last node whose key is less than or equal the target key
 */
func (sl *SkipList) seek(key []byte, withPrev bool) (*skipListNode, []*skipListNode) {
    if sl == nil || sl.head == nil {
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
    if withPrev {
        prev = make([]*skipListNode, sl.maxLevel)
    }
    for level >= 0 {
        if withPrev {
            prev[level] = node
        }
        cmp := bytes.Compare(key, node.next[level].key)
        if cmp > 0 {
            node = node.next[level]
            level = len(node.next) - 1
        } else if cmp == 0 {
            return node, prev
        } else {
            level--
        }
    }
    return node, prev
}