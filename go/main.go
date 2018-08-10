package main

import (
    "time"
    "golang.org/x/exp/rand"
    "bytes"
    "fmt"
    "sync"
    "math"
)

func init() {
    rand.Read(maxKey)
}

// need to call rand.Read(maxKey) during init
var maxKey = make([]byte, 256)

func main() {
    var beginTime, endTime time.Time
    var re []byte

    beginTime = time.Now()
    re = Sol3()
    endTime = time.Now()
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

// binary search(take byte slice as big number)
func Sol3() []byte {
    type searchResult struct {
        index int
        result []byte
    }
    min := make([]byte, 256)
    max := make([]byte, 256)
    for i := 0; i < 256; i++ {
        max[i] = 255
    }

    mid, over := getMid(min, max)
    for {
        ch := make(chan *searchResult, len(mid))
        for i := 0; i < len(mid); i++ {
            go func(index int) {
                ch <- &searchResult{index,Search(mid[index])}
            }(i)
        }
        tmp := min
        var lower int
        for j := 0; j < len(mid); j++ {
            re := <-ch
            if re.result != nil && bytes.Compare(re.result, tmp) > 0 {
                tmp = re.result
                lower = re.index
            }
        }
        min = tmp
        if lower < len(mid)-1 {
            max = mid[lower+1]
        }
        if over {
            if Search(max) != nil {
                return max
            } else {
                return min
            }
        }
        mid, over = getMid(min, max)
    }
}
func getMid(min []byte, max []byte) ([][]byte, bool) {
    n := 512
    _, s := sub(max, min)
    delta := shift(s, 9)
    var mid [][]byte
    over := false
    if bytes.Compare(delta, make([]byte, 256)) != 0 {
        mid = make([][]byte, n-1)
        for i := 0; i < len(mid); i++ {
            _, product := mul(delta, uint16(i+1))
            _, mid[i] = add(min, product)
        }
    } else {
        over = true
        one := make([]byte, 256)
        one[len(one)-1] = 1
        _, s := add(min, one)
        for bytes.Compare(s, max) != 0 {
            mid = append(mid, s)
            _, s = add(s, one)
        }
    }
    return mid, over
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

// return a/(2^n)
func shift(a []byte, n uint) []byte {
    if a == nil || n == 0 {
        return a
    }
    re := append(make([]byte, n/8), a[0:uint(len(a))-n/8]...)
    n = n % 8
    if n == 0 {
        return re
    }

    for i := len(a) - 1; i >= 0; i-- {
        if i > 0 {
            re[i] = (re[i-1]%byte(math.Exp2(float64(n))))*byte(math.Exp2(float64(8-n))) + re[i]/byte(math.Exp2(float64(n)))
        } else {
            re[i] = re[i] / byte(math.Exp2(float64(n)))
        }
    }
    return re
}

// return a*n
func mul(a []byte, n uint16) (uint32, []byte) {
    if a == nil {
        return 0, nil
    }
    re := make([]byte, len(a))
    if n == 0 {
        return 0, re
    }

    var carry uint32 = 0
    var temp uint32 = 0
    for i := len(a) - 1; i >= 0; i-- {
        temp = uint32(a[i])*uint32(n) + carry
        if temp >= 256 {
            carry = temp / 256
            temp %= 256
        } else {
            carry = 0
        }
        re[i] = byte(temp)
    }
    return carry, re
}
