
#include <iostream>
using namespace std;

#include "leveldb/db.h"
#include <inttypes.h>

extern "C" {


    struct DbIter {
        const char *k;
        const char *v;
    };


    void* grebi_leveldb_open(const char *path);
    void grebi_leveldb_close(void* db);

    void grebi_leveldb_put(void *dbptr, const char *k, int64_t klen, const char *v, int64_t vlen);
    const char *grebi_leveldb_get(void *dbptr, const char *k, int64_t klen);

    DbIter grebi_leveldb_iter_start(void *dbptr);
    DbIter grebi_leveldb_iter_next(void *dbptr);
    void grebi_leveldb_iter_end(void *dbptr);
}


void* grebi_leveldb_open(const char *path) {

    cout << "C++ grebi_leveldb: opening db at " << path << endl;

    leveldb::Options options;
    options.create_if_missing=true;
    //options.max_open_files ??


    leveldb::DB* db = nullptr;
    leveldb::Status status = leveldb::DB::Open(options, path, &db);

    if(!status.ok()) {
        cerr << "failed opening db" << endl;
        exit(1);
        return 0;
    }

    cout << "C++ grebi_leveldb: opened db at " << path << endl;

    return db;
}

void grebi_leveldb_close(void *dbptr) {
    delete ((leveldb::DB *) dbptr);
}


static std::string curValue;

const char *grebi_leveldb_get(void *dbptr, const char *k, int64_t k_len) {

    leveldb::DB *db = (leveldb::DB *) dbptr;

    leveldb::Slice k_slice(k, k_len);

    leveldb::Status s = db->Get(leveldb::ReadOptions(), k_slice, &curValue);
    if(s.ok()) {
        curValue.push_back(0);
        return curValue.c_str();
    }

    return nullptr;

}

void grebi_leveldb_put(void *dbptr, const char *k, int64_t k_len, const char *v, int64_t v_len) {

    leveldb::DB *db = (leveldb::DB *) dbptr;

    leveldb::Slice k_slice(k, k_len);
    leveldb::Slice v_slice(v, v_len);

    db->Put(leveldb::WriteOptions(), k_slice, v_slice);
}




static leveldb::Iterator *it = nullptr;
static std::string it_k;
static std::string it_v;

DbIter grebi_leveldb_iter_start(void *dbptr) {

    leveldb::DB *db = (leveldb::DB *) dbptr;

    it = db->NewIterator(leveldb::ReadOptions());
    it->SeekToFirst();
    if(it->Valid()) {
        it_k = it->key().ToString();
        it_v = it->value().ToString();

        DbIter iter;
        iter.k = it_k.c_str();
        iter.v = it_v.c_str();
        return iter;
    } else {
        exit(3);
    }
}

DbIter grebi_leveldb_iter_next(void *dbptr) {

    leveldb::DB *db = (leveldb::DB *) dbptr;

    it->Next();
    if(it->Valid()) {
        it_k = it->key().ToString();
        it_v = it->value().ToString();

        DbIter iter;
        iter.k = it_k.c_str();
        iter.v = it_v.c_str();
        return iter;
    } else {
        DbIter iter;
        iter.k = nullptr;
        iter.v = nullptr;
        return iter;
    }
}

void grebi_leveldb_iter_end(void *dbptr) {

    leveldb::DB *db = (leveldb::DB *) dbptr;

    delete it;
    it = nullptr;
}

