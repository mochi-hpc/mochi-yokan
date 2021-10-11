/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _ADMIN_H
#define _ADMIN_H

#include "yokan/admin.h"

typedef struct yk_admin {
   margo_instance_id mid;
   hg_id_t           create_database_id;
   hg_id_t           open_database_id;
   hg_id_t           close_database_id;
   hg_id_t           destroy_database_id;
   hg_id_t           list_databases_id;
} yk_admin;

#endif
