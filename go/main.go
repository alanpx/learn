package main

import (
    "time"
    "fmt"
)

func main() {
}
func selectProblem() {
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
