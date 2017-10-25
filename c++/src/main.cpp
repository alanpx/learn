#include <sys/time.h>
#include <iostream>
#include <thread>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

using namespace std;

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
    //testLevelDB();
    cout << "123";
}