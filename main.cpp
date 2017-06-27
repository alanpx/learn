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
#include <cassert>
#include <thread>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

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
void testLevelDB() {
    using namespace leveldb;
    DB* db;
    Options options;
    options.create_if_missing = true;
    DB::Open(options, "/tmp/testdb", &db);
    WriteBatch batch;
    batch.Put("01", "01");
    db->Write(WriteOptions(), &batch);
    /*
    string val;
    db->Get(leveldb::ReadOptions(), "name2", &val);
    cout << val;
     */
}
int main() {
    testLevelDB();
}