/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <assert.h>
#include <rkv/rkv-client.h>
#include <rkv/rkv-database.h>

#define FATAL(...) \
    do { \
        margo_critical(__VA_ARGS__); \
        exit(-1); \
    } while(0)

int main(int argc, char** argv)
{
    if(argc != 4) {
        fprintf(stderr,"Usage: %s <server address> <provider id> <database id>\n", argv[0]);
        exit(-1);
    }

    rkv_return_t ret;
    hg_return_t hret;
    const char* svr_addr_str = argv[1];
    uint16_t    provider_id  = atoi(argv[2]);
    const char* id_str       = argv[3];
    if(strlen(id_str) != 36) {
        FATAL(MARGO_INSTANCE_NULL,"id should be 36 character long");
    }

    margo_instance_id mid = margo_init("tcp", MARGO_CLIENT_MODE, 0, 0);
    assert(mid);

    hg_addr_t svr_addr;
    hret = margo_addr_lookup(mid, svr_addr_str, &svr_addr);
    if(hret != HG_SUCCESS) {
        FATAL(mid,"margo_addr_lookup failed for address %s", svr_addr_str);
    }

    rkv_client_t rkv_clt;
    rkv_database_handle_t rkv_rh;

    margo_info(mid, "Creating RKV client");
    ret = rkv_client_init(mid, &rkv_clt);
    if(ret != RKV_SUCCESS) {
        FATAL(mid,"rkv_client_init failed (ret = %d)", ret);
    }

    rkv_database_id_t database_id;
    rkv_database_id_from_string(id_str, &database_id);

    margo_info(mid, "Creating database handle for database %s", id_str);
    ret = rkv_database_handle_create(
            rkv_clt, svr_addr, provider_id,
            database_id, &rkv_rh);
    if(ret != RKV_SUCCESS) {
        FATAL(mid,"rkv_database_handle_create failed (ret = %d)", ret);
    }

#if 0
    margo_info(mid, "Saying Hello to server");
    ret = rkv_say_hello(rkv_rh);
    if(ret != RKV_SUCCESS) {
        FATAL(mid,"rkv_say_hello failed (ret = %d)", ret);
    }

    margo_info(mid, "Computing sum");
    int32_t result;
    ret = rkv_compute_sum(rkv_rh, 45, 23, &result);
    if(ret != RKV_SUCCESS) {
        FATAL(mid,"rkv_compute_sum failed (ret = %d)", ret);
    }
    margo_info(mid, "45 + 23 = %d", result);
#endif

    margo_info(mid, "Releasing database handle");
    ret = rkv_database_handle_release(rkv_rh);
    if(ret != RKV_SUCCESS) {
        FATAL(mid,"rkv_database_handle_release failed (ret = %d)", ret);
    }

    margo_info(mid, "Finalizing client");
    ret = rkv_client_finalize(rkv_clt);
    if(ret != RKV_SUCCESS) {
        FATAL(mid,"rkv_client_finalize failed (ret = %d)", ret);
    }

    hret = margo_addr_free(mid, svr_addr);
    if(hret != HG_SUCCESS) {
        FATAL(mid,"Could not free address (margo_addr_free returned %d)", hret);
    }

    margo_finalize(mid);

    return 0;
}
