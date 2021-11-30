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

// The raw_data structure is used to send and receive raw data.
// When de-serializing into a raw_data structure, hg_proc_raw_data
// will look whether the data pointer is NULL. If it is, it will
// allocate the memory to store the raw data. If it is not, it will
// try to copy the data into the existing memory, returning HG_NOMEM
// if the size is too small.
//
// When freeing the structure with margo_free_input or margo_free_output,
// free will be called on the data pointer. It is therefore important
// to set it to NULL if the buffer was provided by the caller and should
// not be freed.
typedef struct raw_data {
    size_t size;
    char*  data;
} raw_data;

// id_list is used for both arrays of ids and  arrays of sizes
// (ids are uint64_t anyway).
// This structure's serialization functions work in the same way
// as the ones for raw_data, so precautions must be taken to
// correctly set the fields before deserializing into such a structure.
typedef struct uint64_list {
    size_t count;
    union {
        yk_id_t*  ids;
        uint64_t* sizes;
    };
} uint64_list;

static inline hg_return_t hg_proc_yk_database_id_t(hg_proc_t proc, yk_database_id_t *id);
static inline hg_return_t hg_proc_yk_id_t(hg_proc_t proc, yk_id_t *id);
static inline hg_return_t hg_proc_uint64_list(hg_proc_t proc, uint64_list* list);
static inline hg_return_t hg_proc_raw_data(hg_proc_t proc, raw_data* raw);

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

/* exists (direct) */
MERCURY_GEN_PROC(exists_direct_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((raw_data)(keys))\
        ((uint64_list)(sizes)))
MERCURY_GEN_PROC(exists_direct_out_t,
        ((raw_data)(flags))\
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

/* length (direct) */
MERCURY_GEN_PROC(length_direct_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((raw_data)(keys))\
        ((uint64_list)(sizes)))
MERCURY_GEN_PROC(length_direct_out_t,
        ((uint64_list)(sizes))\
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

/* doc_erase */
MERCURY_GEN_PROC(doc_erase_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((uint64_list)(ids)))
MERCURY_GEN_PROC(doc_erase_out_t,
        ((int32_t)(ret)))

/* doc_store */
MERCURY_GEN_PROC(doc_store_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((uint64_t)(count))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(doc_store_out_t,
        ((int32_t)(ret))\
        ((uint64_list)(ids)))

/* doc_store (direct) */
MERCURY_GEN_PROC(doc_store_direct_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((uint64_list)(sizes))\
        ((raw_data)(docs)))
MERCURY_GEN_PROC(doc_store_direct_out_t,
        ((int32_t)(ret))\
        ((uint64_list)(ids)))

/* doc_update */
MERCURY_GEN_PROC(doc_update_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((uint64_list)(ids))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(doc_update_out_t,
        ((int32_t)(ret)))

/* doc_load */
MERCURY_GEN_PROC(doc_load_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((uint64_list)(ids))\
        ((uint64_t)(offset))\
        ((uint64_t)(size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk))\
        ((hg_bool_t)(packed)))
MERCURY_GEN_PROC(doc_load_out_t,
        ((int32_t)(ret)))

/* doc_length */
MERCURY_GEN_PROC(doc_length_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((uint64_list)(ids)))
MERCURY_GEN_PROC(doc_length_out_t,
        ((uint64_list)(sizes))\
        ((int32_t)(ret)))

/* doc_list */
MERCURY_GEN_PROC(doc_list_in_t,
        ((yk_database_id_t)(db_id))\
        ((int32_t)(mode))\
        ((hg_string_t)(coll_name))\
        ((hg_bool_t)(packed))\
        ((uint64_t)(count))\
        ((yk_id_t)(from_id))\
        ((uint64_t)(filter_size))\
        ((uint64_t)(offset))\
        ((uint64_t)(docs_buf_size))\
        ((hg_string_t)(origin))\
        ((hg_bulk_t)(bulk)))
MERCURY_GEN_PROC(doc_list_out_t,
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

static inline hg_return_t hg_proc_uint64_list(hg_proc_t proc, uint64_list* in)
{
    hg_return_t ret;
    hg_size_t count;
    switch(hg_proc_get_op(proc)) {
        case HG_ENCODE:
            ret = hg_proc_hg_size_t(proc, &in->count);
            if(ret != HG_SUCCESS) return ret;
            ret = hg_proc_raw(proc, in->ids, in->count*sizeof(*(in->ids)));
            if (ret != HG_SUCCESS) return ret;
            break;
        case HG_DECODE:
            ret = hg_proc_hg_size_t(proc, &count);
            if(ret != HG_SUCCESS) return ret;
            if(!in->ids) {
                in->ids  = (yk_id_t*)malloc(count*sizeof(*(in->ids)));
                in->count = count;
            }
            if(in->count >= count) {
                in->count = count;
                ret       = hg_proc_raw(proc, in->ids, in->count*sizeof(*(in->ids)));
            } else {
                ret = HG_NOMEM;
            }
            if(ret != HG_SUCCESS) return ret;
            break;
        case HG_FREE:
            free(in->ids);
            break;
        default:
            break;
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_raw_data(hg_proc_t proc, raw_data* in)
{
    hg_return_t ret;
    hg_size_t size;
    switch(hg_proc_get_op(proc)) {
    case HG_ENCODE:
        ret = hg_proc_hg_size_t(proc, &in->size);
        if(ret != HG_SUCCESS) return ret;
        ret = hg_proc_raw(proc, in->data, in->size);
        if(ret != HG_SUCCESS) return ret;
        break;
    case HG_DECODE:
        ret = hg_proc_hg_size_t(proc, &size);
        if(ret != HG_SUCCESS) return ret;
        if(in->data == NULL) {
            in->data = (char*)malloc(size);
            in->size = size;
        }
        if(in->size >= size) {
            ret = hg_proc_raw(proc, in->data, size);
            if(ret != HG_SUCCESS) return ret;
        } else {
            return HG_NOMEM;
        }
        break;
    case HG_FREE:
        free(in->data);
        break;
    default:
        break;
    }
    return HG_SUCCESS;
}

// LCOV_EXCL_STOP

#endif
