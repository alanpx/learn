package main

import (
    "fmt"
    "sync"
)

func main() {
    //testDefer()
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            fmt.Println(i)
        }()
    }
    wg.Wait()
    fmt.Println("done")
}
func testDefer() {
    var fs = [4]func(){}
    for i := 0; i < 4; i++ {
        defer fmt.Println("defer i= ", i)                       //这是一个i作为参数传进去的输出，因为i是int型，所以遵循一个规则值拷贝的传递，还有defer是倒序执行的，所以先后输出3,2,1,0，跟下面的defer交替执行4次
        defer func() { fmt.Println("defer_closure i = ", i) }() //执行完下面的代码后，到了该defer了，这也是一个匿名函数，同样的也没有参数，也没有定义i，所以这也是个闭包，用的也是外面的i，所以先输出4，接着执行上面的defer，这样反复执行4次
        fs[i] = func() { fmt.Println("closure i = ", i) }       //把相应的4个匿名函数存到function类型的slice里，因为这是个匿名函数，又没有参数，且也没有定义i，所以i就是外层函数的地址引用，就是for循环的i的地址，执行完for后i的值为4，所以输出4个4
    }
    for _, f := range fs {
        f()
    }
}
