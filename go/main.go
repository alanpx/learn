package main

import (
    "time"
    "golang.org/x/exp/rand"
    "bytes"
    "fmt"
    "sync"
)

func init() {
    rand.Read(maxKey)
}

// need to call rand.Read(maxKey) during init
var maxKey = make([]byte, 256)

func main() {
    fmt.Println(time.Now())
    fmt.Println(bytes.Compare(Sol2(), maxKey))
    fmt.Println(time.Now())
}

func Sol1() []byte {
    key := make([]byte, 256)
    for i := 0; i < 256; i++ {
        var min byte = 0
        var max byte = 255
        for max > min+1 {
            key[i] = min + (max-min)/2
            if Search(key) == nil {
                max = min + (max-min)/2
            } else {
                min = min + (max-min)/2
            }
        }
        key[i] = max
        if Search(key) == nil {
            key[i] = min
        }
    }
    return key
}
func Sol2() []byte {
    key := make([]byte, 256)
    for i := 0; i < 256; i++ {
        var j byte
        ch := make(chan []byte, 256)
        var wg sync.WaitGroup
        for j != 255 {
            key[i] = j
            k := make([]byte, 256)
            copy(k, key)
            wg.Add(1)
            go func() {
                defer wg.Add(-1)
                ch <- Search(k)
            }()
            j++
        }

        wg.Wait()
        var max byte
        for j := 0; j < 255; j++ {
            re := <-ch
            if re != nil && re[i] > max {
                max = re[i]
            }
        }
        key[i] = max
    }
    return key
}
func Search(key []byte) []byte {
    time.Sleep(time.Millisecond * 10)
    if bytes.Compare(key, maxKey) > 0 {
        return nil
    }
    return key
}
