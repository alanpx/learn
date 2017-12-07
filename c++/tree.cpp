#include <string>
#include <vector>
#include <queue>

using namespace std;

struct TreeNode {
    int val;
    TreeNode *left;
    TreeNode *right;
    TreeNode(int x) : val(x), left(nullptr), right(nullptr) {}
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
