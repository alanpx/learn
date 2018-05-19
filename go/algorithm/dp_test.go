package algorithm

import (
    "testing"
)

func TestLongestPalindromicSubstring(t *testing.T) {
    str := []string{"aa", "aba", "abccbd", "abcacbd", "abbacaba"}
    ans := []string{"aa", "aba", "bccb", "bcacb", "bacab"}
    for i := range str {
        re := LongestPalindrome(str[i])
        if re != ans[i] {
            t.Errorf("%s not equals %s\n", re, ans[i])
        }
    }
}
func TestMaximalRectangle(t *testing.T) {
    arr := [][][]byte{
        {{'1'}},
        {{'1', '1'}, {'0', '1'}},
        {{'1', '0', '0', '1', '1'}, {'1', '0', '1', '1', '0'}, {'1', '1', '1', '1', '1'}, {'1', '0', '0', '1', '1'}},
        {{'1', '0', '0', '1', '1'}, {'1', '0', '1', '1', '0'}, {'1', '1', '1', '1', '1'}, {'1', '0', '1', '1', '1'}},
        {{'1', '0', '1', '1', '1'}, {'1', '0', '1', '1', '0'}, {'1', '1', '1', '1', '1'}, {'1', '0', '1', '1', '1'}},
        {{'1', '0', '1', '1', '0'}, {'1', '0', '1', '1', '1'}, {'1', '1', '1', '1', '1'}, {'1', '0', '1', '1', '1'}},
    }
    ans := []int{1, 2, 5, 6, 8, 9}
    for i := range arr {
        re := MaximalRectangle(arr[i])
        if re != ans[i] {
            t.Errorf("%d not equals %d\n", re, ans[i])
        }
    }
}
func TestMaxSumSubmatrix(t *testing.T) {
    arr := [][]int{
        {1, -4, 0, -5},
        {3, -1, 5, -4},
        {-3, 6, 1, 0},
        {1, 3, 3, 1},
    }
    k := []int{-6, -2, 2, 12}
    ans := []int{-8, -2, 2, 12}
    for i := range k {
        re := MaxSumSubmatrix(arr, k[i])
        if re != ans[i] {
            t.Errorf("%d not equals %d\n", re, ans[i])
        }
    }
}
