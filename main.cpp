#include <sys/time.h>
#include <iostream>
#include <fstream>
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
    int maxPathSum(TreeNode* root) {
        if (nullptr == root) return 0;
        auto re = maxPathSumInternal(root);
        auto args = {re.find(0) != re.end() ? re[0] : re[1], re[1], re.find(2) != re.end() ? re[2] : re[1]};
        return max(args);
    }
    /*  注意:路径是单向的,不能有重合节点
     *  将路径分为:
     *    含根双跨路径(含根节点及两个子节点,这种路径只能用于父节点的不含根路径)
     *    含根单跨路径(含根节点及至多一个子节点)
     *    不含根路径
     *  若节点只有一个子节点,则该节点没有含根双跨路径;若节点没有子节点,则该节点没有不含根路径;所有非空节点均有含根单跨路径
     *  返回 map<路径类型,val>
     */
    map<int,int> maxPathSumInternal(TreeNode* root) {
        /*
         *  key 0含根双跨路径 1含根单跨路径 2不含根路径
         *  re[0] = root->val + left[1] + right[1]
         *  re[1] = root->val + max(left[1],right[1],0)
         *  re[2] = max(left[0],left[1],left[2],right[0],right[1],right[2])
         */
        map<int,int> re, left, right;
        re[1] = root->val;
        if (nullptr == root->left && nullptr == root->right) {
            return re;
        }

        if (nullptr != root->left) {
            left = maxPathSumInternal(root->left);
            auto args = {
                    left.find(0) != left.end() ? left[0] : left[1],
                    left[1],
                    left.find(2) != left.end() ? left[2] : left[1]
            };
            re[2] = max(args);
        }
        if (nullptr != root->right) {
            right = maxPathSumInternal(root->right);
            auto args = {
                    re.find(2) != re.end() ? re[2] : right[1],
                    right.find(0) != right.end() ? right[0] : right[1],
                    right[1],
                    right.find(2) != right.end() ? right[2] : right[1]
            };
            re[2] = max(args);
        }
        re[0] = root->val;
        if (!left.empty() && !right.empty()) {
            re[0] += left[1] + right[1];
        }
        auto args = {left.find(1) != left.end() ? left[1] : 0, right.find(1) != right.end() ? right[1] : 0, 0};
        re[1] = root->val + max(args);
        return re;
    };
};
int main() {
    string str;
    ifstream ifile ("/Users/xp/Downloads/str");
    getline(ifile, str);
    ifile.close();
    vector<string> v{"1","2","null","null","3","4"};
    auto root = Tree::genTree(v);
    Solution *s = new Solution();
    cout << s->maxPathSum(root);
    /*
    struct timeval b, e;
    gettimeofday(&b, NULL);
    gettimeofday(&e, NULL);
    cout << (e.tv_sec - b.tv_sec)*1000 + (e.tv_usec - b.tv_usec)/1000 << "ms";
     */
}