#include "config.h"

static const char* available_backends[] = {
    "map",
    "unordered_map",
    "set",
    "unordered_set",
#ifdef YOKAN_HAS_LEVELDB
    "leveldb",
#endif
#ifdef YOKAN_HAS_LMDB
    "lmdb",
#endif
#ifdef YOKAN_HAS_BERKELEYDB
    "berkeleydb",
#endif
#ifdef YOKAN_HAS_ROCKSDB
    "rocksdb",
#endif
#ifdef YOKAN_HAS_GDBM
    "gdbm",
#endif
#ifdef YOKAN_HAS_PMEMKV
    "pmemkv",
#endif
#ifdef YOKAN_HAS_TKRZW
    "tkrzw",
#endif
#ifdef YOKAN_HAS_UNQLITE
    "unqlite",
#endif
    NULL
};

static const char* backend_configs[] = {
    "{\"disable_doc_mixin_lock\":true}",
    "{\"disable_doc_mixin_lock\":true}",
    "{\"disable_doc_mixin_lock\":true}",
    "{\"disable_doc_mixin_lock\":true}",
#ifdef YOKAN_HAS_LEVELDB
    "{\"path\":\"/tmp/leveldb-test\","
    " \"disable_doc_mixin_lock\":true,"
    " \"create_if_missing\":true}",
#endif
#ifdef YOKAN_HAS_LMDB
    "{\"path\":\"/tmp/lmdb-test\","
    " \"disable_doc_mixin_lock\":true,"
    " \"create_if_missing\":true}",
#endif
#ifdef YOKAN_HAS_BERKELEYDB
    "{\"home\":\"/tmp/berkeleydb-test\","
    " \"file\":\"my-bdb\","
    " \"disable_doc_mixin_lock\":true,"
    " \"create_if_missing\":true,"
    " \"type\":\"btree\"}",
#endif
#ifdef YOKAN_HAS_ROCKSDB
    "{\"path\":\"/tmp/rocksdb-test\","
    " \"disable_doc_mixin_lock\":true,"
    " \"create_if_missing\":true}",
#endif
#ifdef YOKAN_HAS_GDBM
    "{\"path\":\"/tmp/gdbm-test\","
    " \"disable_doc_mixin_lock\":true,"
    " \"create_if_missing\":true}",
#endif
#ifdef YOKAN_HAS_PMEMKV
    "{}",
#endif
#ifdef YOKAN_HAS_TKRZW
    "{\"path\":\"/tmp/tkrzw-test\","
    " \"disable_doc_mixin_lock\":true,"
    " \"type\":\"tree\"}",
#endif
#ifdef YOKAN_HAS_UNQLITE
    "{\"path\":\":mem:\","
    " \"disable_doc_mixin_lock\":true,"
    "\"mode\":\"memory\"}",
#endif
    NULL
};

inline static const char* find_backend_config_for(const char* backend) {
    unsigned i=0;
    while(backend_configs[i] != NULL) {
        if(strcmp(backend, available_backends[i]) == 0)
            return backend_configs[i];
        i += 1;
    }
    return NULL;
}

#define SKIP_IF_NOT_IMPLEMENTED(__ret__) \
    do { if(__ret__ == YOKAN_ERR_OP_UNSUPPORTED || __ret__ == YOKAN_ERR_MODE) return MUNIT_SKIP; } while(0)
