#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <margo.h>
#include <yokan/server.h>
#include <yokan/client.h>
#include <yokan/database.h>

int main(int argc, char** argv)
{
    (void)argc; (void)argv;

    // Initialize Margo (na+sm is fine for a single-process demo)
    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    assert(mid);

    hg_addr_t addr;
    hg_return_t hret = margo_addr_self(mid, &addr);
    assert(hret == HG_SUCCESS);
    (void)hret;

    // Provider 1: source — has a map database.
    yk_provider_t src_provider;
    struct yk_provider_args args = YOKAN_PROVIDER_ARGS_INIT;
    const char* src_config = "{ \"database\": { \"type\": \"map\" } }";
    yk_return_t yret = yk_provider_register(
            mid, 1, src_config, &args, &src_provider);
    assert(yret == YOKAN_SUCCESS);
    (void)yret;

    // Provider 2: destination — starts with no database.
    yk_provider_t dst_provider;
    yret = yk_provider_register(mid, 2, "{}", &args, &dst_provider);
    assert(yret == YOKAN_SUCCESS);

    yk_client_t client;
    yret = yk_client_init(mid, &client);
    assert(yret == YOKAN_SUCCESS);

    yk_database_handle_t dbh_src;
    yret = yk_database_handle_create(client, addr, 1, true, &dbh_src);
    assert(yret == YOKAN_SUCCESS);

    // Populate the source database.
    printf("Populating source database...\n");
    for(int i = 0; i < 10; i++) {
        char key[16], value[16];
        sprintf(key,   "key%05d",   i);
        sprintf(value, "value%05d", i);
        yret = yk_put(dbh_src, 0, key, strlen(key), value, strlen(value));
        assert(yret == YOKAN_SUCCESS);
    }
    printf("  Inserted 10 key/value pairs\n");

    // Snapshot the database to a local directory. Imagine this path lives
    // on a parallel filesystem (Lustre, GPFS, etc.) in production. With
    // remove_source = false, the source database stays live.
    const char* snap_dir = "/tmp/yokan-snapshot-example";
    struct yk_snapshot_options snap_opts = { NULL /*extra_config*/,
                                             0    /*xfer_size*/ };
    printf("\nSnapshotting source to %s...\n", snap_dir);
    yret = yk_provider_snapshot_database(
            src_provider, snap_dir, false /*remove_source*/, &snap_opts);
    assert(yret == YOKAN_SUCCESS);
    printf("  Snapshot complete\n");

    // Source database is still live.
    {
        char buf[32]; size_t vsize = sizeof(buf);
        memset(buf, 0, sizeof(buf));
        yret = yk_get(dbh_src, 0, "key00003", 8, buf, &vsize);
        assert(yret == YOKAN_SUCCESS);
        printf("  Source still serves: key00003 -> %.*s\n", (int)vsize, buf);
    }
    yk_database_handle_release(dbh_src);

    // Restore the snapshot into the (empty) destination provider. The
    // restored database will operate against new_root — typically a local
    // SSD path so subsequent writes don't touch the parallel filesystem.
    const char* restored_root = "/tmp/yokan-restored-example";
    struct yk_restore_options rest_opts = {
        restored_root,    // new_root: required
        NULL,             // extra_config
        0                 // xfer_size
    };
    printf("\nRestoring snapshot into provider 2 (working root: %s)...\n",
           restored_root);
    yret = yk_provider_restore_database(dst_provider, snap_dir, &rest_opts);
    assert(yret == YOKAN_SUCCESS);
    printf("  Restore complete\n");

    // Verify all keys round-trip on the destination.
    yk_database_handle_t dbh_dst;
    yret = yk_database_handle_create(client, addr, 2, true, &dbh_dst);
    assert(yret == YOKAN_SUCCESS);

    printf("\nVerifying restored data at destination...\n");
    for(int i = 0; i < 10; i++) {
        char key[16], expected[16], buf[32];
        sprintf(key,      "key%05d",   i);
        sprintf(expected, "value%05d", i);
        size_t vsize = sizeof(buf);
        memset(buf, 0, sizeof(buf));
        yret = yk_get(dbh_dst, 0, key, strlen(key), buf, &vsize);
        assert(yret == YOKAN_SUCCESS);
        assert(vsize == strlen(expected));
        assert(memcmp(buf, expected, vsize) == 0);
    }
    printf("  All 10 key/value pairs successfully restored\n");

    // Clean up.
    yk_database_handle_release(dbh_dst);
    yk_client_finalize(client);
    margo_addr_free(mid, addr);
    margo_finalize(mid);

    printf("\nSnapshot/restore example completed successfully!\n");
    return 0;
}
