#include <sys/time.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <list>
#include <queue>
#include <set>
#include <map>
#include <unordered_map>
#include <initializer_list>

using namespace std;

struct ListNode {
    int val;
    ListNode *next;
    ListNode(int x) : val(x), next(NULL) {}
};
struct TreeNode {
    int val;
    TreeNode *left;
    TreeNode *right;
    TreeNode(int x) : val(x), left(NULL), right(NULL) {}
};
class Tree {
public:
    static TreeNode *genTree(vector<int> lists) {
        if (lists.empty()) return nullptr;
        auto root = new TreeNode(lists[0]);
        queue<TreeNode *> q;
        q.push(root);
        for (auto i = lists.begin() + 1; i != lists.end(); i++) {
            if (nullptr != q.front()->left && nullptr != q.front()->right) {
                q.pop();
            }
            if (nullptr == q.front()->left) {
                q.front()->left = new TreeNode(*i);
                q.push(q.front()->left);
            } else if (nullptr == q.front()->right) {
                q.front()->right = new TreeNode(*i);
                q.push(q.front()->right);
            }
        }
        return root;
    }
    static TreeNode *genTree(vector<string> lists) {
        if (lists.empty() || "null" == lists[0]) return nullptr;
        auto root = new TreeNode(stoi(lists[0]));
        queue<TreeNode *> q;
        q.push(root);
        bool ignoreLeft = false, ignoreRight = false;
        for (auto i = lists.begin() + 1; i != lists.end(); i++) {
            if ((nullptr != q.front()->left || ignoreLeft)
                && (nullptr != q.front()->right || ignoreRight)) {
                q.pop();
                ignoreLeft = false;
                ignoreRight = false;
            }
            if (nullptr == q.front()->left && !ignoreLeft) {
                if ("null" == *i) {
                    ignoreLeft = true;
                } else {
                    q.front()->left = new TreeNode(stoi(*i));
                    q.push(q.front()->left);
                }
            } else if (nullptr == q.front()->right && !ignoreRight) {
                if ("null" == *i) {
                    ignoreRight = true;
                } else {
                    q.front()->right = new TreeNode(stoi(*i));
                    q.push(q.front()->right);
                };
            }
        }
        return root;
    }
};
class LinkList {
public:
    LinkList(initializer_list<int> l) {
        ListNode *last = root, *add;
        for (auto i = l.begin(); i != l.end(); i++) {
            add = new ListNode(*i);
            if (nullptr == root) {
                root = add;
                last = root;
                continue;
            }
            last->next = add;
            last = last->next;
        }
    }
    void print() {
        auto cur = root;
        while (nullptr != cur) {
            cout << cur->val << " ";
            cur = cur->next;
        }
    }
    ListNode *root = nullptr;
};
class Solution {
public:
    bool exist(vector<vector<char>>& board, string word) {
        if (board.empty() || word.empty()) return false;

        auto height = board.size();
        auto width = board[0].size();
        auto len = word.size();
        int i, j, k, i1, j1;
        set<pair<int,int>> s;
        for (i = 0; i < height; i++) {
            for (j = 0; j < width; j++) {
                i1 = i;
                j1 = j;
                k = 0;
                s.clear();
                while (k < len && board[i1][j1] == word[k]) {
                    if (k == len - 1) return true;

                    s.insert(pair<int,int>(i1, j1));
                    bool stepOn = false;
                    if (i1 - 1 >= 0 && board[i1-1][j1] == word[k+1] && s.find(pair<int,int>(i1-1,j1)) == s.end()) {
                        stepOn = true;
                        i1 -= 1;
                        k++;
                        continue;
                    }
                    if (i1 + 1 < height && board[i1+1][j1] == word[k+1] && s.find(pair<int,int>(i1+1,j1)) == s.end()) {
                        stepOn = true;
                        i1 += 1;
                        k++;
                        continue;
                    }
                    if (j1 - 1 >= 0 && board[i1][j1-1] == word[k+1] && s.find(pair<int,int>(i1,j1-1)) == s.end()) {
                        stepOn = true;
                        j1 -= 1;
                        k++;
                        continue;
                    }
                    if (j1 + 1 < width && board[i1][j1+1] == word[k+1] && s.find(pair<int,int>(i1,j1+1)) == s.end()) {
                        stepOn = true;
                        j1 += 1;
                        k++;
                        continue;
                    }
                    if (!stepOn) {
                        if (i1 - 1 >= 0) s.insert(pair<int,int>(i1-1, j1));
                        if (i1 + 1 < height) s.insert(pair<int,int>(i1+1, j1));
                        if (j1 - 1 >= 0) s.insert(pair<int,int>(i1, j1-1));
                        if (j1 + 1 < width) s.insert(pair<int,int>(i1, j1+1));
                    }
                    if ((i - 1 >= 0 && s.find(pair<int, int>(i-1, j)) == s.end()) ||
                        (i + 1 < height && s.find(pair<int, int>(i+1, j)) == s.end()) ||
                        (j - 1 >= 0 && s.find(pair<int, int>(i, j-1)) == s.end()) ||
                        (j + 1 < width && s.find(pair<int, int>(i, j+1)) == s.end())) {
                        i1 = i;
                        j1 = j;
                        k = 0;
                        continue;
                    }
                    break;
                }
            }
        }
        return false;
    }
    bool existInternal(vector<vector<char>>& board, set<pair<int,int>> &s, pair<int,int> &start, string &word, int k) {
        if (board.empty() || word.empty()) return false;

        auto height = board.size();
        auto width = board[0].size();
        auto len = word.size();
        int i = start.first, j = start.second;
        if (k == len - 1) return true;

        s.insert(pair<int, int>(i, j));
        if (i - 1 >= 0 && board[i - 1][j] == word[k + 1] && s.find(pair<int, int>(i - 1, j)) == s.end()) {
            pair<int, int> p(i - 1, j);
            if (existInternal(board, s, p, word, k + 1)) return true;
        }
        if (i + 1 < height && board[i + 1][j1] == word[k + 1] && s.find(pair<int, int>(i + 1, j)) == s.end()) {
            pair<int, int> p(i + 1, j);
            if (existInternal(board, s, p, word, k + 1)) return true;
        }
        if (j - 1 >= 0 && board[i][j - 1] == word[k + 1] && s.find(pair<int, int>(i, j - 1)) == s.end()) {
            pair<int, int> p(i, j - 1);
            if (existInternal(board, s, p, word, k + 1)) return true;
        }
        if (j + 1 < width && board[i][j + 1] == word[k + 1] && s.find(pair<int, int>(i, j + 1)) == s.end()) {
            pair<int, int> p(i, j + 1);
            if (existInternal(board, s, p, word, k + 1)) return true;
        }
        return false;
    }
};
int main() {
    Solution *s = new Solution();
    vector<vector<char>> vec{
            {'A','B','C','E'},
            {'S','F','E','S'},
            {'A','D','E','E'}
    };
    cout << s->exist(vec, "ABCESEEEFS");
}