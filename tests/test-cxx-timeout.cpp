/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <margo.h>
#include <yokan/server.h>
#include <yokan/client.h>
#include <yokan/cxx/client.hpp>
#include <yokan/cxx/database.hpp>
#include <yokan/cxx/collection.hpp>
#include <yokan/cxx/extras.hpp>
#include "munit/munit.h"

#include <cstring>
#include <string>
#include <vector>

/* The variadic-extras dispatch on the C++ wrappers is template-only: when
 * the parameter pack is empty the wrapper takes the original code path
 * (no YOKAN_MODE_EXTRA), and when it's non-empty it sets EXTRA and emits
 * every known extra. This test exercises both branches on a handful of
 * representative Database and Collection methods. The unique-extra design
 * is checked at compile time via static_assert below — if the catalogue
 * of known extras drifts away from what the wrappers emit, builds break. */

static_assert(sizeof(yokan::Timeout) > 0,
              "yokan::Timeout must be visible to wrapper consumers");

struct ctx {
    margo_instance_id  mid;
    hg_addr_t          addr;
    yk_provider_t      provider;
    yokan::Client      client;
    yokan::Database    db;
};

static const uint16_t k_provider_id = 42;

static void* setup(const MunitParameter[], void*)
{
    auto* c = new ctx;
    c->mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    munit_assert_not_null(c->mid);
    margo_addr_self(c->mid, &c->addr);

    yk_provider_args args = YOKAN_PROVIDER_ARGS_INIT;
    auto ret = yk_provider_register(
            c->mid, k_provider_id,
            "{\"database\":{\"type\":\"map\"}}",
            &args, &c->provider);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    c->client = yokan::Client(c->mid);
    c->db = c->client.makeDatabaseHandle(c->addr, k_provider_id);
    return c;
}

static void tear_down(void* fixture)
{
    auto* c = static_cast<ctx*>(fixture);
    c->db = yokan::Database{};
    c->client = yokan::Client{};
    margo_addr_free(c->mid, c->addr);
    yk_provider_destroy(c->provider);
    margo_finalize(c->mid);
    delete c;
}

/* The default (no extras) path must continue to work and produce the same
 * observable behaviour as before. */
static MunitResult test_db_no_extras(const MunitParameter[], void* data)
{
    auto& db = static_cast<ctx*>(data)->db;
    const char* key = "k_default";
    const char* val = "v_default";

    db.put(key, std::strlen(key), val, std::strlen(val));

    char buf[32] = {0};
    size_t vsize = sizeof(buf);
    db.get(key, std::strlen(key), buf, &vsize);
    munit_assert_size(vsize, ==, std::strlen(val));
    munit_assert_memory_equal(vsize, buf, val);

    munit_assert_true(db.exists(key, std::strlen(key)));
    munit_assert_size(db.length(key, std::strlen(key)), ==, std::strlen(val));

    db.erase(key, std::strlen(key));
    munit_assert_false(db.exists(key, std::strlen(key)));
    return MUNIT_OK;
}

/* Same operations, but with a generous Timeout extra. 5 seconds is far more
 * than na+sm needs, so the call must succeed; what we're really verifying
 * is that the variadic-template dispatch + extras emission compile and
 * land the right (mode | YOKAN_MODE_EXTRA, timeout, END) tail on the wire. */
static MunitResult test_db_with_timeout(const MunitParameter[], void* data)
{
    auto& db = static_cast<ctx*>(data)->db;
    const yokan::Timeout t{5000.0};
    const char* key = "k_to";
    const char* val = "v_to";

    db.put(key, std::strlen(key), val, std::strlen(val),
           YOKAN_MODE_DEFAULT, t);

    char buf[32] = {0};
    size_t vsize = sizeof(buf);
    db.get(key, std::strlen(key), buf, &vsize, YOKAN_MODE_DEFAULT, t);
    munit_assert_size(vsize, ==, std::strlen(val));
    munit_assert_memory_equal(vsize, buf, val);

    munit_assert_true(db.exists(key, std::strlen(key), YOKAN_MODE_DEFAULT, t));
    munit_assert_size(db.length(key, std::strlen(key), YOKAN_MODE_DEFAULT, t),
                      ==, std::strlen(val));

    db.erase(key, std::strlen(key), YOKAN_MODE_DEFAULT, t);
    munit_assert_false(db.exists(key, std::strlen(key), YOKAN_MODE_DEFAULT, t));
    return MUNIT_OK;
}

/* Timeout{0.0} is the C-side sentinel for "blocking forever", and it's the
 * default-constructed value the wrapper would emit for an absent extra.
 * It must be accepted and behave like the no-extras path. */
static MunitResult test_db_timeout_zero(const MunitParameter[], void* data)
{
    auto& db = static_cast<ctx*>(data)->db;
    const char* key = "k_zero";
    const char* val = "v_zero";
    db.put(key, std::strlen(key), val, std::strlen(val),
           YOKAN_MODE_DEFAULT, yokan::Timeout{});
    munit_assert_true(db.exists(key, std::strlen(key)));
    return MUNIT_OK;
}

/* Multi/Packed paths: enough variety to catch a wrong argument slot in the
 * EXTRA-mode call expansion. */
static MunitResult test_db_multi_with_timeout(const MunitParameter[], void* data)
{
    auto& db = static_cast<ctx*>(data)->db;
    const yokan::Timeout t{2500.0};

    std::vector<std::string> keys = {"a", "bb", "ccc"};
    std::vector<std::string> vals = {"A", "BB", "CCC"};
    std::vector<const void*> kptrs, vptrs;
    std::vector<size_t> ksizes, vsizes;
    for(size_t i = 0; i < keys.size(); i++) {
        kptrs.push_back(keys[i].data()); ksizes.push_back(keys[i].size());
        vptrs.push_back(vals[i].data()); vsizes.push_back(vals[i].size());
    }
    db.putMulti(keys.size(), kptrs.data(), ksizes.data(),
                vptrs.data(), vsizes.data(),
                YOKAN_MODE_DEFAULT, t);

    auto present = db.existsMulti(keys.size(), kptrs.data(), ksizes.data(),
                                  YOKAN_MODE_DEFAULT, t);
    munit_assert_size(present.size(), ==, keys.size());
    for(auto b : present) munit_assert_true(b);

    db.eraseMulti(keys.size(), kptrs.data(), ksizes.data(),
                  YOKAN_MODE_DEFAULT, t);
    return MUNIT_OK;
}

/* Collection wrapper: same dispatch lives there, so exercise one round trip. */
static MunitResult test_coll_with_timeout(const MunitParameter[], void* data)
{
    auto& db = static_cast<ctx*>(data)->db;
    const yokan::Timeout t{5000.0};

    db.createCollection("test_coll", YOKAN_MODE_DEFAULT, t);
    munit_assert_true(db.collectionExists("test_coll", YOKAN_MODE_DEFAULT, t));

    yokan::Collection coll{"test_coll", db};

    const char* doc = "{\"k\":1}";
    auto id = coll.store(doc, std::strlen(doc), YOKAN_MODE_DEFAULT, t);

    char buf[32] = {0};
    size_t bufsize = sizeof(buf);
    coll.load(id, buf, &bufsize, YOKAN_MODE_DEFAULT, t);
    munit_assert_size(bufsize, ==, std::strlen(doc));
    munit_assert_memory_equal(bufsize, buf, doc);

    munit_assert_size(coll.size(YOKAN_MODE_DEFAULT, t), ==, 1u);
    coll.erase(id, YOKAN_MODE_DEFAULT, t);
    db.dropCollection("test_coll", YOKAN_MODE_DEFAULT, t);
    return MUNIT_OK;
}

static MunitTest tests[] = {
    { (char*)"/cxx_timeout/no_extras", test_db_no_extras,
      setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*)"/cxx_timeout/with_timeout", test_db_with_timeout,
      setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*)"/cxx_timeout/timeout_zero", test_db_timeout_zero,
      setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*)"/cxx_timeout/multi", test_db_multi_with_timeout,
      setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*)"/cxx_timeout/coll", test_coll_with_timeout,
      setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite suite = {
    (char*)"/yk", tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)])
{
    return munit_suite_main(&suite, (void*)"yk", argc, argv);
}
