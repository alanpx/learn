package algorithm

// Given a string s, find the longest palindromic substring in s.
func LongestPalindrome(str string) string {
    l := len(str)
    odd := make([]bool, l)
    even := make([]bool, l)
    var oddRadius, evenRadius, oddCenter, evenCenter int
    for radius := 0; radius <= l/2; radius++ {
        for c := 0; c < l; c++ {
            if radius == 0 {
                odd[c] = true
                even[c] = true
            } else {
                if c-radius >= 0 && c+radius < l {
                    odd[c] = odd[c] && (str[c-radius] == str[c+radius])
                } else {
                    odd[c] = false
                }
                if c-radius+1 >= 0 && c+radius < l {
                    even[c] = even[c] && (str[c-radius+1] == str[c+radius])
                } else {
                    even[c] = false
                }
            }
            if odd[c] && radius > oddRadius {
                oddRadius = radius
                oddCenter = c
            }
            if even[c] && radius > evenRadius {
                evenRadius = radius
                evenCenter = c
            }
        }
    }
    if oddRadius >= evenRadius {
        return str[oddCenter-oddRadius: oddCenter+oddRadius+1]
    } else {
        return str[evenCenter-evenRadius+1: evenCenter+evenRadius+1]
    }
}
// Given a 2D binary matrix filled with 0's and 1's, find the largest rectangle containing only 1's and return its area.
func MaximalRectangle(matrix [][]byte) int {
    type point struct {
        i, j int
    }
    // re map right bottom point to left top point for all rectangles which contain only "1"
    re := make(map[point]map[point]bool)
    var maxArea, area int
    for i := range matrix {
        for j := range matrix[i] {
            if matrix[i][j] == '0' {
                continue
            }
            re[point{i,j}] = map[point]bool{point{i,j}: true}
            if maxArea == 0 {
                maxArea = 1
            }

            if i > 0 {
                jLeftTop := j-1
                for jLeftTop >= 0 && matrix[i][jLeftTop] == '1' {
                    jLeftTop--
                }
                m, exist := re[point{i-1, j}]
                if exist {
                    for p := range m {
                        if p.j > jLeftTop {
                            re[point{i, j}][p] = true
                            area = (i-p.i+1) * (j-p.j+1)
                            if area > maxArea {
                                maxArea = area
                            }
                        }
                    }
                }
            }

            if j > 0 {
                iLeftTop := i-1
                for iLeftTop >= 0 && matrix[iLeftTop][j] == '1' {
                    iLeftTop--
                }
                m, exist := re[point{i, j-1}]
                if exist {
                    for p := range m {
                        if p.i > iLeftTop {
                            re[point{i, j}][p] = true
                            area = (i-p.i+1) * (j-p.j+1)
                            if area > maxArea {
                                maxArea = area
                            }
                        }
                    }
                }
            }
        }
    }

    return maxArea
}
// Given a non-empty 2D matrix matrix and an integer k, find the max sum of a rectangle in the matrix such that its sum is no larger than k.
func MaxSumSubmatrix(matrix [][]int, k int) int {
    type point struct {
        i, j int
    }
    // re map right bottom point to left top point for all sub matrix
    re := make(map[point]map[point]int)
    var maxSum int
    var hasSum bool
    for i := range matrix {
        for j := range matrix[i] {
            re[point{i,j}] = map[point]int{point{i,j}: matrix[i][j]}
            if (!hasSum || matrix[i][j] > maxSum) && matrix[i][j] <= k {
                maxSum = matrix[i][j]
                hasSum = true
            }
            if hasSum && maxSum == k {
                return maxSum
            }

            if i > 0 {
                s := 0
                for jLeftTop := j; jLeftTop >= 0; jLeftTop-- {
                    s += matrix[i][jLeftTop]
                    m, exist := re[point{i-1, j}]
                    if !exist {
                        break
                    }
                    for p := range m {
                        if p.j != jLeftTop {
                            continue
                        }
                        re[point{i, j}][p] = m[p] + s
                        if (!hasSum || re[point{i, j}][p] > maxSum) && re[point{i, j}][p] <= k {
                            maxSum = re[point{i, j}][p]
                            hasSum = true
                        }
                        if hasSum && maxSum == k {
                            return maxSum
                        }
                    }
                }
            }

            if j > 0 {
                s := 0
                for iLeftTop := i; iLeftTop >= 0; iLeftTop-- {
                    s += matrix[iLeftTop][j]
                    m, exist := re[point{i, j-1}]
                    if !exist {
                        continue
                    }
                    for p := range m {
                        if p.i != iLeftTop {
                            continue
                        }
                        re[point{i, j}][p] = m[p] + s
                        if (!hasSum || re[point{i, j}][p] > maxSum) && re[point{i, j}][p] <= k {
                            maxSum = re[point{i, j}][p]
                            hasSum = true
                        }
                        if hasSum && maxSum == k {
                            return maxSum
                        }
                    }
                }
            }
        }
    }

    return maxSum
}