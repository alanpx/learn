package algorithm

import (
    "testing"
    "strconv"
    "math/rand"
    "bytes"
)

func TestSkipList(t *testing.T) {
    const N = 1000
    data := make(map[int][]byte)
    del := make(map[int]bool)
    sl := NewSkipList()
    for i := 0; i < N; i++ {
        num := rand.Int()
        k := []byte(strconv.Itoa(num))
        sl.Add(k, k)
        data[num] = k
    }
    i := 0
    for k, v := range data {
        key := []byte(strconv.Itoa(k))
        if i % 3 == 0 {
            v := append([]byte("a"), v...)
            sl.Update(key, v)
            data[k] = v
        } else if i % 3 == 1 {
            sl.Rem(key)
            del[k] = true
        }
        i++
    }
    for k, v := range data {
        val, ok := sl.Get([]byte(strconv.Itoa(k)))
        _, isDel := del[k]
        if isDel {
            if ok {
                t.Fatalf("got deleted key: %d", k)
            }
        } else {
            if !ok || bytes.Compare(v, val) != 0 {
                t.Fatalf("get key:%d, expected: %s, got: %s", k, string(v), string(val))
            }
        }
    }
}
