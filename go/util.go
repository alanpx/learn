package main

type slice []int
func (s *slice) insert(value int, pos int) {
	*s = append((*s)[:pos], append(slice{value}, (*s)[pos:]...)...)
}