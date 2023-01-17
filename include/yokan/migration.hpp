/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_MIGRATION_H
#define __YOKAN_MIGRATION_H

#include <list>
#include <string>
#include <yokan/common.h>

namespace yokan {

/**
 * @brief A MigrationHandle is an abstract class representing
 * an object that one can request from a database using
 * database.startMigration(). Its roles are (1) to lock all
 * accesses to the database until its destruction (acting like
 * a lock guard), (2) to provide a list of files that need to
 * be migrated, (3) to cleanup any temporary files
 * used during migration upon destruction, and (4) to mark
 * the database as migrated.
 */
class MigrationHandle {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~MigrationHandle() = default;

    /**
     * @brief Get the path relative to which
     * the files returned by getFiles are located.
     */
    virtual std::string getRoot() const = 0;

    /**
     * @brief Get a list of files to migrate.
     * The file names must be relative to the root.
     */
    virtual std::list<std::string> getFiles() const = 0;

    /**
     * @brief Mark the migration as canceled.
     */
    virtual void cancel() = 0;
};

}

#endif
