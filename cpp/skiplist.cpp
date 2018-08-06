using namespace std;

class SkipList {
    SkipList *head;
    int total;
    int maxLevel;
    int size;
};
struct SkipListNode {
    vector<char> key;
    vector<char> val;
    SkipListNode *next[];
};