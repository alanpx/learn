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
    var beginTime, endTime time.Time
    var re []byte

    fmt.Println(maxKey)
    fmt.Println()
    beginTime = time.Now()
    re = Sol3()
    endTime = time.Now()
    fmt.Println(re)
    fmt.Printf("Sol2 cmp: %d, time: %s \n", bytes.Compare(maxKey, re), endTime.Sub(beginTime))
}

// bytewise search
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

// concurrent bytewise search
func Sol2() []byte {
    key := make([]byte, 256)
    for i := 0; i < 256; i++ {
        ch := make(chan []byte, 256)
        var wg sync.WaitGroup
        for j := 0; j < 256; j++ {
            key[i] = byte(j)
            k := make([]byte, 256)
            copy(k, key)
            wg.Add(1)
            go func() {
                defer wg.Add(-1)
                ch <- Search(k)
            }()
        }

        wg.Wait()
        var max byte
        for j := 0; j < 256; j++ {
            re := <-ch
            if re != nil && re[i] > max {
                max = re[i]
            }
        }
        key[i] = max
    }
    return key
}

// binary search
func Sol3() []byte {
    min := make([]byte, 256)
    max := make([]byte, 256)
    for i := 0; i < 256; i++ {
        max[i] = 255
    }
    _, s := sub(max, min)
    _, mid := add(min,shift(s))
    for bytes.Compare(min, mid) != 0 {
        fmt.Println(min)
        fmt.Println(mid)
        fmt.Println(max)
        fmt.Println()
        if Search(mid) == nil {
            max = mid
        } else {
            min = mid
        }
        _, s = sub(max, min)
        _, mid = add(min,shift(s))
    }
    if Search(max) != nil {
        return max
    } else {
        return min
    }
}
func Search(key []byte) []byte {
    time.Sleep(time.Millisecond * 10)
    if bytes.Compare(key, maxKey) > 0 {
        return nil
    }
    return key
}

// return a+b
func add(a []byte, b []byte) (byte, []byte) {
    if a == nil || b == nil || len(a) != len(b) {
        return 0, nil
    }
    var carry byte = 0
    var temp int16 = 0
    re := make([]byte, len(a))
    for i := len(a) - 1; i >= 0; i-- {
        temp = int16(a[i]) + int16(b[i]) + int16(carry)
        if temp >= 256 {
            temp -= 256
            carry = 1
        } else {
            carry = 0
        }
        re[i] = byte(temp)
    }
    return carry, re
}

// return a-b
func sub(a []byte, b []byte) (byte, []byte) {
    if a == nil || b == nil || len(a) != len(b) {
        return 0, nil
    }
    var borrow byte = 0
    var temp int16 = 0
    re := make([]byte, len(a))
    for i := len(a) - 1; i >= 0; i-- {
        temp = int16(a[i]) - int16(b[i]) - int16(borrow)
        if temp < 0 {
            temp += 256
            borrow = 1
        } else {
            borrow = 0
        }
        re[i] = byte(temp)
    }
    return borrow, re
}

// right shift a by 1 bit
func shift(a []byte) []byte {
    if a == nil {
        return nil
    }
    re := make([]byte, len(a))
    for i := len(a) - 1; i >= 0; i-- {
        if i > 0 {
            re[i] = (a[i-1]%2)*128 + a[i]/2
        } else {
            re[i] /= 2
        }
    }
    return re
}
