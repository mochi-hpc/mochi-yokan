/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MIGRATION_H
#define __MIGRATION_H
#ifdef YOKAN_HAS_REMI
#include <nlohmann/json.hpp>
#include <yokan/common.h>
#include "provider.hpp"
#include <remi/remi-common.h>
#include <remi/remi-client.h>
#include <remi/remi-server.h>

using json = nlohmann::json;

static inline int32_t before_migration_cb(remi_fileset_t fileset, void* uargs)
{
    // the goal this callback is just to make sure the required metadata
    // is available and there  isn't any database with the same name yet,
    // so we can do the migration safely.
    yk_provider_t provider = (yk_provider_t)uargs;

    const char* uuid = nullptr;
    const char* name = nullptr;
    const char* type = nullptr;
    const char* db_config = nullptr;
    const char* migration_config = nullptr;

    int rret;
    rret = remi_fileset_get_metadata(fileset, "uuid", &uuid);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;
    rret = remi_fileset_get_metadata(fileset, "name", &name);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;
    rret = remi_fileset_get_metadata(fileset, "type", &type);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;
    rret = remi_fileset_get_metadata(fileset, "db_config", &db_config);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;
    rret = remi_fileset_get_metadata(fileset, "migration_config", &migration_config);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;

    uuid_t uu;
    if(uuid_parse((char*)uuid, uu) != 0)
        return YOKAN_ERR_INVALID_DATABASE;

    try {
        auto j1 = json::parse(db_config);
        auto j2 = json::parse(migration_config);
    } catch (...) {
        return YOKAN_ERR_INVALID_CONFIG;
    }

    if(name[0] && provider->db_names.count(name)) {
        return YOKAN_ERR_KEY_EXISTS;
    }

    if(!yokan::DatabaseFactory::hasBackendType(type))
        return YOKAN_ERR_INVALID_BACKEND;

    return YOKAN_SUCCESS;
}

static inline int32_t after_migration_cb(remi_fileset_t fileset, void* uargs)
{
    yk_provider_t provider = (yk_provider_t)uargs;

    const char* uuid = nullptr;
    const char* name = nullptr;
    const char* type = nullptr;
    const char* db_config = nullptr;
    const char* migration_config = nullptr;

    remi_fileset_get_metadata(fileset, "uuid", &uuid);
    remi_fileset_get_metadata(fileset, "name", &name);
    remi_fileset_get_metadata(fileset, "type", &type);
    remi_fileset_get_metadata(fileset, "db_config", &db_config);
    remi_fileset_get_metadata(fileset, "migration_config", &migration_config);

    auto json_db_config = json::parse(db_config);
    auto json_mig_config = json::parse(migration_config);

    yk_database_id_t id;
    uuid_parse((char*)uuid, id.uuid);

    std::list<std::string> files;
    int rret = remi_fileset_walkthrough(fileset,
        [](const char* filename, void* uargs) {
            static_cast<std::list<std::string>*>(uargs)->emplace_back(filename);
        }, &files);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;

    size_t root_size = 0;
    std::vector<char> root;
    rret = remi_fileset_get_root(fileset, nullptr, &root_size);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;
    root.resize(root_size);
    rret = remi_fileset_get_root(fileset, root.data(), &root_size);
    if(rret != REMI_SUCCESS) return YOKAN_ERR_FROM_REMI;

    auto root_str = std::string{root.data()};
    for(auto& filename : files) {
        filename = root_str + "/" + filename;
    }

    yk_database_t database;
    auto status = yokan::DatabaseFactory::recoverDatabase(
        type, name, db_config, migration_config, files, &database);
    if(status != yokan::Status::OK) return (int32_t)status;

    provider->dbs[id] = database;
    if(name && name[0]) {
        provider->db_names[std::string(name)] = id;
    }

    return 0;
}

#endif
#endif
