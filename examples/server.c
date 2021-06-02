/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <assert.h>
#include <stdio.h>
#include <margo.h>
#include <rkv/rkv-server.h>

int main(int argc, char** argv)
{
    (void)argc;
    (void)argv;
    margo_instance_id mid = margo_init("tcp", MARGO_SERVER_MODE, 0, 0);
    assert(mid);

    hg_addr_t my_address;
    margo_addr_self(mid, &my_address);
    char addr_str[128];
    size_t addr_str_size = 128;
    margo_addr_to_string(mid, addr_str, &addr_str_size, my_address);
    margo_addr_free(mid,my_address);
    margo_info(mid, "Server running at address %s, with provider id 42", addr_str);

    struct rkv_provider_args args = RKV_PROVIDER_ARGS_INIT;

    rkv_provider_register(mid, 42, &args, RKV_PROVIDER_IGNORE);

    margo_wait_for_finalize(mid);

    return 0;
}
