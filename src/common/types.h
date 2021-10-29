/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _PARAMS_H
#define _PARAMS_H

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>
#include "yokan/common.h"
#include <stdlib.h>

// LCOV_EXCL_START

typedef struct id_list {
    size_t count;
    yk_id_t* ids;
} id_list;

static inline hg_return_t hg_proc_yk_database_id_t(hg_proc_t proc, yk_database_id_t *id);
static inline hg_return_t hg_proc_yk_id_t(hg_proc_t proc, yk_id_t *id);
static inline hg_return_t hg_proc_id_list(hg_proc_t proc, id_list* list);

/* Admin RPC types */

MERCURY_GEN_PROC(open_database_in_t,
        ((hg_string_t)(type))\
        ((hg_string_t)(config))\
        ((hg_string_t)(token)))

MERCURY_GEN_PROC(open_database_out_t,
        ((int32_t)(ret))\
        ((yk_database_id_t)(id)))

MERCURY_GEN_PROC(close_database_in_t,
        ((hg_string_t)(token))\
        ((yk_database_id_t)(id)))

MERCURY_GEN_PROC(close_database_out_t,
        ((int32_t)(ret)))

MERCURY_GEN_PROC(destroy_database_in_t,
        ((hg_string_t)(token))\
        ((yk_database_id_t)(id)))

MERCURY_GEN_PROC(destroy_database_out_t,
        ((int32_t)(ret)))

MERCURY_GEN_PROC(list_databases_in_t,
        ((hg_string_t)(token))\
        ((hg_size_t)(max_ids)))

typedef struct list_databases_out_t {
    int32_t ret;
    hg_size_t count;
    yk_database_id_t* ids;
} list_databases_out_t;

static inline hg_return_t hg_proc_list_databases_out_t(hg_proc_t proc, void *data)
{
    list_databases_out_t* out = (list_databases_out_t*)data;
    hg_return_t ret;

    ret = hg_proc_hg_int32_t(proc, &(out->ret));
    if(ret != HG_SUCCESS) return ret;

    ret = hg_proc_hg_size_t(proc, &(out->count));
    if(ret != HG_SUCCESS) return ret;

    switch(hg_proc_get_op(proc)) {
    case HG_DECODE:
        out->ids = (yk_database_id_t*)calloc(out->count, sizeof(*(out->ids)));
        /* fall through */
    case HG_ENCODE:
        if(out->ids)
            ret = hg_proc_memcpy(proc, out->ids, sizeof(*(out->ids))*out->count);
        break;
    case HG_FREE:
        free(out->ids);
        break;
    }
    return ret;
}

/* Client RPC types */

/* count */
MERCURY_GEN_PROC(count_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode)))
MERCURY_GEN_PROC(count_out_t,
        ((int32_t)(ret))\
        ((uint64_t)(count)))

/* exists */
MERCURY_GEN_PROC(exists_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((uint64_t)(count))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(exists_out_t,
        ((int32_t)(ret)))

/* length */
MERCURY_GEN_PROC(length_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((uint64_t)(count))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(length_out_t,
        ((int32_t)(ret)))

/* put */
MERCURY_GEN_PROC(put_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((uint64_t)(count))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))

MERCURY_GEN_PROC(put_out_t,
        ((int32_t)(ret)))

/* get */
MERCURY_GEN_PROC(get_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((uint64_t)(count))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bool_t)(packed))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(get_out_t,
        ((int32_t)(ret)))

/* erase */
MERCURY_GEN_PROC(erase_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((uint64_t)(count))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(erase_out_t,
        ((int32_t)(ret)))

/* list_keys */
MERCURY_GEN_PROC(list_keys_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_bool_t)(packed))\
        ((uint64_t)(count))\
        ((uint64_t)(from_ksize))\
        ((uint64_t)(filter_size))\
        ((uint64_t)(offset))\
        ((uint64_t)(keys_buf_size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(list_keys_out_t,
        ((int32_t)(ret)))

/* list_keyvals */
MERCURY_GEN_PROC(list_keyvals_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_bool_t)(packed))\
        ((uint64_t)(count))\
        ((uint64_t)(from_ksize))\
        ((uint64_t)(filter_size))\
        ((uint64_t)(offset))\
        ((uint64_t)(keys_buf_size))\
        ((uint64_t)(vals_buf_size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(list_keyvals_out_t,
        ((int32_t)(ret)))


/* coll_create */
MERCURY_GEN_PROC(coll_create_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name)))
MERCURY_GEN_PROC(coll_create_out_t,
        ((int32_t)(ret)))

/* coll_drop */
MERCURY_GEN_PROC(coll_drop_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name)))
MERCURY_GEN_PROC(coll_drop_out_t,
        ((int32_t)(ret)))

/* coll_exists */
MERCURY_GEN_PROC(coll_exists_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name)))
MERCURY_GEN_PROC(coll_exists_out_t,
        ((int32_t)(ret))\
        ((uint8_t)(exists)))

/* coll_erase */
MERCURY_GEN_PROC(coll_erase_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((id_list)(ids)))
MERCURY_GEN_PROC(coll_erase_out_t,
        ((int32_t)(ret)))

/* coll_last_id */
MERCURY_GEN_PROC(coll_last_id_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name)))
MERCURY_GEN_PROC(coll_last_id_out_t,
        ((int32_t)(ret))\
        ((yk_id_t)(last_id)))

/* coll_size */
MERCURY_GEN_PROC(coll_size_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name)))
MERCURY_GEN_PROC(coll_size_out_t,
        ((int32_t)(ret))\
        ((uint64_t)(size)))

/* coll_store */
MERCURY_GEN_PROC(coll_store_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((uint64_t)(count))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(coll_store_out_t,
        ((int32_t)(ret))\
        ((id_list)(ids)))

/* coll_update */
MERCURY_GEN_PROC(coll_update_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((id_list)(ids))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(coll_update_out_t,
        ((int32_t)(ret)))

/* coll_load */
MERCURY_GEN_PROC(coll_load_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((id_list)(ids))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk))\
        ((hg_bool_t)(packed)))
MERCURY_GEN_PROC(coll_load_out_t,
        ((int32_t)(ret)))

/* Extra hand-coded serialization functions */

static inline hg_return_t hg_proc_yk_id_t(
        hg_proc_t proc, yk_id_t* id)
{
    return hg_proc_uint64_t(proc, id);
}

static inline hg_return_t hg_proc_yk_database_id_t(
        hg_proc_t proc, yk_database_id_t *id)
{
    return hg_proc_memcpy(proc, id, sizeof(*id));
}

static inline hg_return_t hg_proc_id_list(hg_proc_t proc, id_list* in)
{
    hg_return_t ret;
    ret = hg_proc_hg_size_t(proc, &in->count);
    if(ret != HG_SUCCESS) return ret;
    if(in->count) {
        switch(hg_proc_get_op(proc)) {
        case HG_ENCODE:
            ret = hg_proc_raw(proc, in->ids, in->count*sizeof(*(in->ids)));
            if (ret != HG_SUCCESS) return ret;
            break;
        case HG_DECODE:
            in->ids  = (yk_id_t*)malloc(in->count*sizeof(*(in->ids)));
            ret      = hg_proc_raw(proc, in->ids, in->count*sizeof(*(in->ids)));
            if(ret != HG_SUCCESS) return ret;
            break;
        case HG_FREE:
            free(in->ids);
            break;
        default:
            break;
        }
    }
    return HG_SUCCESS;
}

// LCOV_EXCL_STOP

#endif
