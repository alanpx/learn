package main

type slice []int
func (s *slice) insert(value int, pos int) {
	a := slice{value}
	if len(*s) >= pos + 1 {
		a = append(a, (*s)[pos:]...)
	}
	*s = append((*s)[:pos], a...)
}