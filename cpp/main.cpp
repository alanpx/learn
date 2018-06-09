#include <iostream>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#define HAVE_CXX_STDHEADERS 1 // for db_cxx.h
#include "db_cxx.h"

using namespace std;
void testLevelDB() {
    using leveldb::DB;
    using leveldb::Options;
    using leveldb::WriteOptions;
    using leveldb::WriteBatch;
    DB *db;
    Options options;
    options.create_if_missing = true;
    DB::Open(options, "/tmp/testdb", &db);
    WriteBatch batch;
    batch.Put("01", "01");
    db->Write(WriteOptions(), &batch);
    string val;
    db->Get(leveldb::ReadOptions(), "name2", &val);
    cout << val;
}
void testBDB() {
    Db db(nullptr, 0);
    u_int32_t oFlags = DB_CREATE;
    db.open(nullptr,    // Transaction pointer
            "/Users/xp/devspace/data/bdb.db", // Database file name
            nullptr,    // Optional logical database name
            DB_BTREE,   // Database access method
            oFlags,     // Open flags
            0);         // File mode (using defaults)

    string key = "key";
    string val = "value";
    Dbt k((void *)key.c_str(), (u_int32_t)key.size()+1);
    Dbt v((void *)val.c_str(), (u_int32_t)val.size()+1);
    db.put(nullptr, &k, &v, 0);

    char val1[10];
    Dbt v1(&val1, 10);
    db.get(nullptr, &k, &v1, 0);
    cout << key << " " << (char *)v1.get_data() << " " << v1.get_size() << endl;

    db.close(0);
}
int main() {
    testBDB();
}
