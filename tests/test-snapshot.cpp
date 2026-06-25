/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "config.h"

#include <stdio.h>
#include <string.h>
#include <margo.h>
#include <yokan/server.h>
#include <yokan/client.h>
#include <yokan/database.h>
#include "available-backends.h"
#include "munit/munit.h"

struct test_context {
    margo_instance_id mid;
    hg_addr_t         addr;
    yk_client_t       yokan_client;
    yk_provider_t     src_provider;   // has a database
    yk_provider_t     dst_provider;   // starts empty, gets restored into
    const char*       backend;
    char              snap_dir[128];
    char              restored_root[128];
};

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void)user_data;
    margo_instance_id mid;
    hg_addr_t         addr;
    hg_return_t       hret;
    yk_return_t       yret;
    const char*       backend = munit_parameters_get(params, "backend");

    mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    munit_assert_not_null(mid);

    margo_set_global_log_level(MARGO_LOG_INFO);
    margo_set_log_level(mid, MARGO_LOG_INFO);

    hret = margo_addr_self(mid, &addr);
    munit_assert_int(hret, ==, HG_SUCCESS);

    // src provider with a database
    yk_provider_t src_provider;
    struct yk_provider_args args = YOKAN_PROVIDER_ARGS_INIT;
    auto src_config = make_provider_config(backend);
    yret = yk_provider_register(mid, 1, src_config.c_str(), &args, &src_provider);
    munit_assert_int(yret, ==, YOKAN_SUCCESS);

    // dst provider with no database — restore will populate it
    yk_provider_t dst_provider;
    yret = yk_provider_register(mid, 2, "{}", &args, &dst_provider);
    munit_assert_int(yret, ==, YOKAN_SUCCESS);

    yk_client_t yokan_client;
    yret = yk_client_init(mid, &yokan_client);
    munit_assert_int(yret, ==, YOKAN_SUCCESS);

    struct test_context* context = (struct test_context*)calloc(1, sizeof(*context));
    munit_assert_not_null(context);
    context->mid           = mid;
    context->addr          = addr;
    context->yokan_client  = yokan_client;
    context->src_provider  = src_provider;
    context->dst_provider  = dst_provider;
    context->backend       = backend;
    snprintf(context->snap_dir,      sizeof(context->snap_dir),
             "/tmp/yokan-snap-%s",     backend);
    snprintf(context->restored_root, sizeof(context->restored_root),
             "/tmp/yokan-restored-%s", backend);

    // clean up any leftover state from a prior run
    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s %s",
             context->snap_dir, context->restored_root);
    int sysret = system(rm_cmd);
    (void)sysret;

    return context;
}

static void test_context_tear_down(void* fixture)
{
    struct test_context* context = (struct test_context*)fixture;
    margo_addr_free(context->mid, context->addr);
    yk_client_finalize(context->yokan_client);
    margo_finalize(context->mid);

    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s %s",
             context->snap_dir, context->restored_root);
    int sysret = system(rm_cmd);
    (void)sysret;

    free(context);
}

static MunitResult test_snapshot_roundtrip(const MunitParameter params[], void* data)
{
    (void)params;
    struct test_context* context = (struct test_context*)data;
    yk_return_t ret;

    const char* backend = context->backend;
    bool stores_kv = !(strcmp(backend, "log") == 0 || strcmp(backend, "array") == 0);
    bool stores_values = (strcmp(backend, "set") != 0)
                      && (strcmp(backend, "unordered_set") != 0);

    yk_database_handle_t dbh_src;
    ret = yk_database_handle_create(context->yokan_client,
                                    context->addr, 1, true, &dbh_src);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    if(stores_kv) {
        for(int i = 0; i < 10; i++) {
            char key[16], value[16];
            sprintf(key,   "key%05d",   i);
            sprintf(value, "value%05d", i);
            size_t ksize = strlen(key);
            size_t vsize = strlen(value) * (stores_values ? 1 : 0);
            ret = yk_put(dbh_src, 0, key, ksize, value, vsize);
            if(ret == YOKAN_ERR_OP_UNSUPPORTED) ret = YOKAN_SUCCESS;
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }
    }

    // snapshot with remove_source = false: source must stay live
    struct yk_snapshot_options snap_opts = { /* extra_config */ NULL,
                                             /* xfer_size    */ 0 };
    ret = yk_provider_snapshot_database(
        context->src_provider, context->snap_dir, false, &snap_opts);
    if(ret == YOKAN_ERR_OP_UNSUPPORTED) {
        // backend doesn't implement startMigration (e.g. berkeleydb, null)
        yk_database_handle_release(dbh_src);
        return MUNIT_SKIP;
    }
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // source still serves
    if(stores_kv) {
        char key[16], buf[32];
        sprintf(key, "key%05d", 3);
        size_t ksize = strlen(key);
        size_t vsize = sizeof(buf);
        memset(buf, 0, sizeof(buf));
        ret = yk_get(dbh_src, 0, key, ksize, buf, &vsize);
        if(ret != YOKAN_ERR_OP_UNSUPPORTED) {
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            if(stores_values) munit_assert_string_equal(buf, "value00003");
        }
    }
    yk_database_handle_release(dbh_src);

    // restore into the dst provider, copying files to a fresh working dir
    struct yk_restore_options rest_opts = {
        /* new_root     */ context->restored_root,
        /* extra_config */ NULL,
        /* xfer_size    */ 0
    };
    ret = yk_provider_restore_database(
        context->dst_provider, context->snap_dir, &rest_opts);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // verify dst now has the same keys
    yk_database_handle_t dbh_dst;
    ret = yk_database_handle_create(context->yokan_client,
                                    context->addr, 2, true, &dbh_dst);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    if(stores_kv) {
        for(int i = 0; i < 10; i++) {
            char key[16], expected[16], buf[32];
            sprintf(key,      "key%05d",   i);
            sprintf(expected, "value%05d", i);
            size_t ksize = strlen(key);
            size_t vsize = sizeof(buf);
            memset(buf, 0, sizeof(buf));
            ret = yk_get(dbh_dst, 0, key, ksize, buf, &vsize);
            if(ret == YOKAN_ERR_OP_UNSUPPORTED) continue;
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            if(stores_values) {
                munit_assert_int(vsize, ==, strlen(expected));
                munit_assert_string_equal(buf, expected);
            }
        }
    }
    yk_database_handle_release(dbh_dst);
    return MUNIT_OK;
}

static MunitResult test_snapshot_remove_source(const MunitParameter params[], void* data)
{
    (void)params;
    struct test_context* context = (struct test_context*)data;
    yk_return_t ret;

    yk_database_handle_t dbh;
    ret = yk_database_handle_create(context->yokan_client,
                                    context->addr, 1, true, &dbh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    bool stores_kv = !(strcmp(context->backend, "log") == 0
                       || strcmp(context->backend, "array") == 0);
    bool stores_values = (strcmp(context->backend, "set") != 0)
                      && (strcmp(context->backend, "unordered_set") != 0);

    if(stores_kv) {
        ret = yk_put(dbh, 0, "k", 1, "v", 1 * (stores_values ? 1 : 0));
        if(ret == YOKAN_ERR_OP_UNSUPPORTED) ret = YOKAN_SUCCESS;
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
    }

    struct yk_snapshot_options snap_opts = { NULL, 0 };
    ret = yk_provider_snapshot_database(
        context->src_provider, context->snap_dir, true /*remove_source*/, &snap_opts);
    if(ret == YOKAN_ERR_OP_UNSUPPORTED) {
        yk_database_handle_release(dbh);
        return MUNIT_SKIP;
    }
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // source database should now be detached
    if(stores_kv) {
        ret = yk_put(dbh, 0, "x", 1, "y", 1 * (stores_values ? 1 : 0));
        munit_assert_int(ret, ==, YOKAN_ERR_INVALID_DATABASE);
    }

    yk_database_handle_release(dbh);
    return MUNIT_OK;
}

static MunitResult test_restore_requires_new_root(const MunitParameter params[], void* data)
{
    (void)params;
    struct test_context* context = (struct test_context*)data;
    yk_return_t ret;

    // warm up the source DB so backends like unqlite actually create
    // a backing file before we try to snapshot it
    yk_database_handle_t dbh;
    ret = yk_database_handle_create(context->yokan_client,
                                    context->addr, 1, true, &dbh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    bool stores_values = (strcmp(context->backend, "set") != 0)
                      && (strcmp(context->backend, "unordered_set") != 0);
    ret = yk_put(dbh, 0, "warmup", 6, "x", 1 * (stores_values ? 1 : 0));
    if(ret != YOKAN_ERR_OP_UNSUPPORTED) munit_assert_int(ret, ==, YOKAN_SUCCESS);
    yk_database_handle_release(dbh);

    // snapshot first (we need a real snapshot dir to read the manifest from)
    struct yk_snapshot_options snap_opts = { NULL, 0 };
    ret = yk_provider_snapshot_database(
        context->src_provider, context->snap_dir, false, &snap_opts);
    if(ret == YOKAN_ERR_OP_UNSUPPORTED) return MUNIT_SKIP;
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // restore with NULL options or NULL new_root must fail with INVALID_ARGS
    ret = yk_provider_restore_database(context->dst_provider, context->snap_dir, NULL);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    struct yk_restore_options rest_opts = { NULL, NULL, 0 };
    ret = yk_provider_restore_database(context->dst_provider, context->snap_dir, &rest_opts);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_restore_bad_path(const MunitParameter params[], void* data)
{
    (void)params;
    struct test_context* context = (struct test_context*)data;
    struct yk_restore_options rest_opts = {
        context->restored_root, NULL, 0
    };
    yk_return_t ret = yk_provider_restore_database(
        context->dst_provider, "/tmp/this-path-does-not-exist-yokan", &rest_opts);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*)"/snapshot-roundtrip", test_snapshot_roundtrip,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*)"/snapshot-remove-source", test_snapshot_remove_source,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*)"/restore-requires-new-root", test_restore_requires_new_root,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*)"/restore-bad-path", test_restore_bad_path,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*)"/yk/snapshot", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*)"yk", argc, argv);
}
