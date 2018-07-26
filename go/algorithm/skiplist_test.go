package algorithm

import (
    "testing"
    "strconv"
    "math/rand"
    "bytes"
    "fmt"
)

func TestSkipList(t *testing.T) {
    const N = 10
    data := make(map[int][]byte)
    del := make(map[int]bool)
    sl := SkipList{}
    for i := 0; i < N; i++ {
        num := int(rand.Int31n(10))
        k := []byte(strconv.Itoa(num))
        sl.Add(k, k)
        data[num] = k
        if i % 3 == 0 {
            v := append([]byte("a"), k...)
            sl.Update(k, v)
            data[num] = v
        } else if i % 3 == 1 {
            sl.Rem(k)
            del[i] = true
        }
    }
    fmt.Println(sl.String())
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
