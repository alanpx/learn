#include <iostream>
#include <initializer_list>

using namespace std;

struct ListNode {
    int val;
    ListNode *next;
    ListNode(int x) : val(x), next(nullptr) {}
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
