/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "config.h"
#include "provider.hpp"
#include "migration.hpp"
#include "../common/logging.h"
#include "yokan/backend.hpp"
#include "yokan/migration.hpp"
#include "yokan/server.h"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <list>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

constexpr const char* k_manifest_name = "yokan-snapshot.json";
constexpr int         k_manifest_version = 1;

/* Copy one entry from the migration handle's root into the destination data
 * subdirectory. Entries ending in '/' (or the literal "/" marker used by
 * on-disk backends like leveldb/rocksdb/lmdb) name a directory and are copied
 * recursively. Everything else is copied as a single file. */
yk_return_t copy_one(const fs::path& src_root,
                     const fs::path& dst_data,
                     const std::string& rel,
                     margo_instance_id mid)
{
    std::error_code ec;
    const bool is_dir_marker = !rel.empty() && rel.back() == '/';

    if(is_dir_marker) {
        const std::string trimmed =
            (rel == "/") ? std::string{} : rel.substr(0, rel.size() - 1);
        fs::path src = trimmed.empty() ? src_root : src_root / trimmed;
        fs::path dst = trimmed.empty() ? dst_data : dst_data / trimmed;
        fs::create_directories(dst, ec);
        if(ec) {
            YOKAN_LOG_ERROR(mid, "create_directories(%s) failed: %s",
                            dst.c_str(), ec.message().c_str());
            return YOKAN_ERR_IO;
        }
        fs::copy(src, dst,
                 fs::copy_options::recursive | fs::copy_options::overwrite_existing,
                 ec);
    } else {
        fs::path src = src_root / rel;
        fs::path dst = dst_data / rel;
        fs::create_directories(dst.parent_path(), ec);
        if(ec) {
            YOKAN_LOG_ERROR(mid, "create_directories(%s) failed: %s",
                            dst.parent_path().c_str(), ec.message().c_str());
            return YOKAN_ERR_IO;
        }
        fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
    }

    if(ec) {
        YOKAN_LOG_ERROR(mid, "failed to copy snapshot entry %s: %s",
                        rel.c_str(), ec.message().c_str());
        return YOKAN_ERR_IO;
    }
    return YOKAN_SUCCESS;
}

} // anonymous namespace

extern "C" yk_return_t yk_provider_snapshot_database(
        yk_provider_t provider,
        const char* dest_path_cstr,
        bool remove_source,
        const struct yk_snapshot_options* options)
{
    (void)options; // xfer_size / extra_config reserved for future use

    if(!provider || !dest_path_cstr || !*dest_path_cstr)
        return YOKAN_ERR_INVALID_ARGS;

    auto database = provider->db;
    if(!database) return YOKAN_ERR_INVALID_DATABASE;

    fs::path dest_path = dest_path_cstr;
    fs::path dest_data = dest_path / "data";
    std::error_code ec;

    // create destination directory (must not already contain a snapshot)
    fs::create_directories(dest_data, ec);
    if(ec) {
        YOKAN_LOG_ERROR(provider->mid,
            "snapshot: failed to create destination %s: %s",
            dest_data.c_str(), ec.message().c_str());
        return YOKAN_ERR_IO;
    }

    // acquire MigrationHandle: holds the database lock and (for in-memory
    // backends) materializes the data to a temporary file we can copy from.
    std::unique_ptr<yokan::MigrationHandle> mh;
    auto status = database->startMigration(mh);
    if(status != yokan::Status::OK)
        return static_cast<yk_return_t>(status);

    // Capture identity before mh is destroyed (dtor may invalidate the db).
    const std::string type_str   = database->type();
    const std::string config_str = database->config();
    const auto        files_list = mh->getFiles();
    const fs::path    src_root   = mh->getRoot();

    // copy each entry from src_root into dest_data
    for(const auto& rel : files_list) {
        yk_return_t r = copy_one(src_root, dest_data, rel, provider->mid);
        if(r != YOKAN_SUCCESS) {
            mh->cancel();
            fs::remove_all(dest_path, ec);
            return r;
        }
    }

    // write manifest
    json manifest;
    manifest["yokan_snapshot_version"] = k_manifest_version;
    manifest["type"]                   = type_str;
    try {
        manifest["db_config"] = json::parse(config_str);
    } catch(...) {
        manifest["db_config"] = config_str;
    }
    {
        json files_json = json::array();
        for(const auto& f : files_list) files_json.push_back(f);
        manifest["files"] = std::move(files_json);
    }

    {
        std::ofstream ofs((dest_path / k_manifest_name).string(),
                          std::ios::binary | std::ios::trunc);
        if(!ofs) {
            YOKAN_LOG_ERROR(provider->mid,
                "snapshot: failed to open manifest %s for writing",
                (dest_path / k_manifest_name).c_str());
            mh->cancel();
            fs::remove_all(dest_path, ec);
            return YOKAN_ERR_IO;
        }
        ofs << manifest.dump(2);
        if(!ofs) {
            mh->cancel();
            fs::remove_all(dest_path, ec);
            return YOKAN_ERR_IO;
        }
    }

    if(!remove_source) {
        // snapshot but keep the source live
        mh->cancel();
        mh.reset();
        return YOKAN_SUCCESS;
    }

    // remove_source: let the migration handle destruct (clears the source),
    // then drop the now-empty database from the provider, mirroring the tail
    // of yk_provider_migrate_database.
    mh.reset();
    database->destroy();
    delete provider->db;
    provider->db = nullptr;
    return YOKAN_SUCCESS;
}

extern "C" yk_return_t yk_provider_restore_database(
        yk_provider_t provider,
        const char* src_path_cstr,
        const struct yk_restore_options* options)
{
    if(!provider || !src_path_cstr || !*src_path_cstr)
        return YOKAN_ERR_INVALID_ARGS;
    if(!options || !options->new_root || !*options->new_root) {
        // v1 requires an explicit working directory for the restored DB.
        // Operating in-place against the snapshot is unsafe for in-memory
        // backends whose recover() consumes the source file.
        YOKAN_LOG_ERROR(provider->mid,
            "restore: options->new_root is required");
        return YOKAN_ERR_INVALID_ARGS;
    }

    fs::path src_path  = src_path_cstr;
    fs::path src_data  = src_path / "data";
    fs::path manifest_path = src_path / "yokan-snapshot.json";
    fs::path new_root  = options->new_root;
    std::error_code ec;

    if(!fs::exists(manifest_path, ec)) {
        YOKAN_LOG_ERROR(provider->mid,
            "restore: manifest %s not found", manifest_path.c_str());
        return YOKAN_ERR_INVALID_ARGS;
    }

    // parse manifest
    json manifest;
    try {
        std::ifstream ifs(manifest_path.string(), std::ios::binary);
        if(!ifs) return YOKAN_ERR_IO;
        ifs >> manifest;
    } catch(...) {
        YOKAN_LOG_ERROR(provider->mid,
            "restore: failed to parse manifest %s", manifest_path.c_str());
        return YOKAN_ERR_INVALID_CONFIG;
    }

    if(!manifest.contains("yokan_snapshot_version")
       || manifest["yokan_snapshot_version"].get<int>() != 1
       || !manifest.contains("type")
       || !manifest.contains("db_config")
       || !manifest.contains("files")
       || !manifest["files"].is_array()) {
        YOKAN_LOG_ERROR(provider->mid,
            "restore: manifest %s has unexpected schema",
            manifest_path.c_str());
        return YOKAN_ERR_INVALID_CONFIG;
    }

    std::string type = manifest["type"].get<std::string>();
    if(!yokan::DatabaseFactory::hasBackendType(type)) {
        YOKAN_LOG_ERROR(provider->mid,
            "restore: backend type \"%s\" is not registered", type.c_str());
        return YOKAN_ERR_INVALID_BACKEND;
    }

    std::list<std::string> files;
    for(const auto& f : manifest["files"]) files.push_back(f.get<std::string>());

    // Merge db_config from manifest with the caller's extra_config (caller wins).
    json db_config_json = manifest["db_config"].is_string()
        ? json::parse(manifest["db_config"].get<std::string>())
        : manifest["db_config"];
    if(options->extra_config && *options->extra_config) {
        try {
            json extra = json::parse(options->extra_config);
            if(extra.is_object() && db_config_json.is_object()) {
                db_config_json.update(extra);
            } else if(extra.is_object()) {
                db_config_json = extra;
            }
        } catch(...) {
            YOKAN_LOG_ERROR(provider->mid,
                "restore: failed to parse options->extra_config as JSON");
            return YOKAN_ERR_INVALID_CONFIG;
        }
    }

    // copy snapshot/data → new_root
    fs::create_directories(new_root, ec);
    if(ec) {
        YOKAN_LOG_ERROR(provider->mid,
            "restore: create_directories(%s) failed: %s",
            new_root.c_str(), ec.message().c_str());
        return YOKAN_ERR_IO;
    }
    for(const auto& rel : files) {
        yk_return_t r = copy_one(src_data, new_root, rel, provider->mid);
        if(r != YOKAN_SUCCESS) return r;
    }

    // If a database is already attached, tear it down before swapping in the
    // recovered one. Bedrock's restore() contract permits a live component.
    if(provider->db) {
        provider->db->destroy();
        delete provider->db;
        provider->db = nullptr;
    }

    // Pass root with a trailing slash so backends that derive their data
    // path via substr(root, find_last_of('/')) (e.g. log) see the intended
    // parent. This matches what REMI delivers on the migration receive path.
    std::string root_for_recover = new_root.string();
    if(root_for_recover.empty() || root_for_recover.back() != '/')
        root_for_recover += '/';

    yk_database_t database = nullptr;
    auto status = yokan::DatabaseFactory::recoverDatabase(
        type, db_config_json.dump(), "{}", root_for_recover, files, &database);
    if(status != yokan::Status::OK) {
        YOKAN_LOG_ERROR(provider->mid,
            "restore: recoverDatabase returned %d", (int)status);
        return static_cast<yk_return_t>(status);
    }

    attach_recovered_database(provider, database, type.c_str());
    return YOKAN_SUCCESS;
}
