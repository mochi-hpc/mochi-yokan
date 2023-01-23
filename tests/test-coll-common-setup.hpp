/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <yokan/server.h>
#include <yokan/admin.h>
#include <yokan/client.h>
#include <yokan/database.h>
#include "available-backends.h"
#include "munit/munit.h"
#include <unordered_map>
#include <vector>
#include <string>

inline bool to_bool(const char* v) {
    if(v == nullptr)
        return false;
    if(strcmp(v, "true") == 0)
        return true;
    if(strcmp(v, "false") == 0)
        return false;
    return false;
}

static size_t g_min_val_size = 1;
static size_t g_max_val_size = 1024;
static size_t g_num_items  = 64;

struct doc_test_context {
    margo_instance_id        mid;
    hg_addr_t                addr;
    yk_admin_t               admin;
    yk_client_t              client;
    yk_provider_t            provider;
    yk_database_id_t         id;
    yk_database_handle_t     dbh;
    int32_t                  mode;
    std::vector<std::string> reference;
    std::string              backend;
};

static const uint16_t provider_id = 42;

static void* doc_test_common_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    yk_return_t      ret;
    margo_instance_id mid;
    hg_addr_t         addr;
    yk_admin_t       admin;
    yk_client_t      client;
    yk_provider_t    provider;
    yk_database_id_t id;
    yk_database_handle_t dbh;

    // read parameters
    const char* min_val_size = munit_parameters_get(params, "min-val-size");
    const char* max_val_size = munit_parameters_get(params, "max-val-size");
    const char* num_items  = munit_parameters_get(params, "num-items");
    const char* backend_type = munit_parameters_get(params, "backend");
    const char* backend_config = find_backend_config_for(backend_type);
    const char* no_rdma = munit_parameters_get(params, "no-rdma");
    if(min_val_size) g_min_val_size = std::atol(min_val_size);
    if(max_val_size) g_max_val_size = std::atol(max_val_size);
    if(num_items)  g_num_items  = std::atol(num_items);
    if(strcmp(backend_type, "set") == 0
    || strcmp(backend_type, "unordered_set") == 0) {
        g_max_val_size = 0;
        g_min_val_size = 0;
    }

    margo_init_info margo_args = MARGO_INIT_INFO_INITIALIZER;
    margo_args.json_config = "{ \"handle_cache_size\" : 0 }";

    // create margo instance
    mid = margo_init_ext("ofi+tcp", MARGO_SERVER_MODE, &margo_args);
    munit_assert_not_null(mid);
    // set log level
    margo_set_global_log_level(MARGO_LOG_WARNING);
    margo_set_log_level(mid, MARGO_LOG_WARNING);
    // get address of current process
    hg_return_t hret = margo_addr_self(mid, &addr);
    munit_assert_int(hret, ==, HG_SUCCESS);
    // register yk provider
    struct yk_provider_args args = YOKAN_PROVIDER_ARGS_INIT;
    args.token = NULL;
    ret = yk_provider_register(
            mid, provider_id, &args,
            &provider);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // create an admin
    ret = yk_admin_init(mid, &admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // open a database using the admin
    ret = yk_open_database(admin, addr,
            provider_id, NULL, backend_type, backend_config, &id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // create a client
    ret = yk_client_init(mid, &client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // create a database handle
    ret = yk_database_handle_create(client,
            addr, provider_id, id, &dbh);
    // create test context
    struct doc_test_context* context = new doc_test_context;
    munit_assert_not_null(context);
    context->mid      = mid;
    context->addr     = addr;
    context->admin    = admin;
    context->client   = client;
    context->provider = provider;
    context->id       = id;
    context->dbh      = dbh;
    context->mode     = 0;
    context->backend  = backend_type;
    if(no_rdma && to_bool(no_rdma))
        context->mode |= YOKAN_MODE_NO_RDMA;
    // create random docs with empty data every 8 values
    for(unsigned i = 0; i < g_num_items; i++) {
        std::string doc;
        size_t size;
        if(g_min_val_size == 0 && g_max_val_size == 0) {
            size = 0;
        } else {
            size = i % 8 == 0 ? 0 : munit_rand_int_range(g_min_val_size, g_max_val_size);
        }
        doc.resize(size);
        for(int j = 0; j < (int)size; j++) {
            char c = (char)munit_rand_int_range(33, 126);
            doc[j] = c;
        }
        context->reference.emplace_back(std::move(doc));
    }

    return context;
}

static void doc_test_common_context_tear_down(void* fixture)
{
    yk_return_t ret;
    struct doc_test_context* context = (struct doc_test_context*)fixture;
    // destroy the database
    ret = yk_destroy_database(context->admin,
            context->addr, provider_id, NULL, context->id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // free the admin
    ret = yk_admin_finalize(context->admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // free the database handle
    ret = yk_database_handle_release(context->dbh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // free the client
    ret = yk_client_finalize(context->client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // free address
    margo_addr_free(context->mid, context->addr);
    // destroy provider (we could let margo finalize it but
    // by calling this function we increase code coverage
    ret = yk_provider_destroy(context->provider);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // we are not checking the return value of the above function with
    // munit because we need margo_finalize to be called no matter what.
    margo_finalize(context->mid);

    delete context;
}
