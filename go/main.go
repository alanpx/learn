package main

import (
    "time"
    "fmt"
    "sync"
)

func main() {
    channelTest()
}
func selectTest() {
    a := 0
    go func() {
        time.Sleep(1500 * time.Millisecond)
        a = 1
        fmt.Println("set a", time.Now())
    }()
loop:
    for {
        if a == 1 {
            break loop
        }
        select {
        case <-time.After(1000*time.Millisecond):
            fmt.Println("tick", time.Now())
        }
    }
}
func channelTest() {
    c := make(chan struct{})
    go func() {
        select {
        case <-c:
            fmt.Println("func1")
        }
    }()
    go func() {
        select {
        case <-c:
            fmt.Println("func2")
        }
    }()
    close(c)
    time.Sleep(3*time.Second)
}
func condTest() {
    c := sync.NewCond(&sync.Mutex{})
    cond := false
    go func() {
        c.L.Lock()
        fmt.Println("lock 1")
        time.Sleep(3*time.Second)
        cond = true
        c.Signal()
        fmt.Println("unlock 1")
        c.L.Unlock()
    }()
    c.L.Lock()
    fmt.Println("lock 2")
    for !cond {
        fmt.Println("tick", time.Now())
        c.Wait()
        fmt.Println("receive signal", time.Now())
    }
    fmt.Println("unlock 2")
    c.L.Unlock()
}