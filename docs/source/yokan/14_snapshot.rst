Database snapshot and restore
==============================

In addition to :doc:`migration <10_migration>`, Yokan supports **snapshotting**
a live database to a directory accessible through the local filesystem, and
later **restoring** it into a provider. This is intended for HPC use cases
where databases run against fast local storage on compute nodes (RAM, NVMe)
and need to be periodically checkpointed to a more durable shared location
such as a parallel filesystem.

Unlike migration, snapshot/restore:

- does **not** require Yokan to be compiled with REMI;
- does **not** require a second running provider;
- preserves the source database by default (no transfer of ownership);
- uses ordinary filesystem I/O via :code:`std::filesystem::copy`, so any
  destination reachable through a POSIX path will work.

How it works
------------

A snapshot is a directory containing:

- A manifest file ``yokan-snapshot.json`` that records the backend type,
  the original database configuration, and the list of files that make up
  the database.
- A ``data/`` subdirectory that holds those files (copies of the backing
  store for on-disk backends, a serialized dump for in-memory backends).

Restore reads the manifest, copies the files out of ``data/`` into a working
root path (typically a local SSD), then re-opens the database from that root
and attaches it to the target provider.

Snapshot API
------------

.. code-block:: c

   struct yk_snapshot_options {
       const char* extra_config;  // reserved for backend-specific knobs
       size_t      xfer_size;     // reserved; chunk size for the copy loop
   };

   yk_return_t yk_provider_snapshot_database(
       yk_provider_t provider,
       const char*   dest_path,
       bool          remove_source,
       const struct yk_snapshot_options* options);

- ``provider``: the source provider whose currently-attached database to snapshot.
- ``dest_path``: a local directory path where the snapshot will be written.
  Created if it does not exist.
- ``remove_source``: if ``true``, the source database is destroyed after a
  successful snapshot (similar to migrate). If ``false``, the database
  remains attached and continues to serve.
- ``options``: optional; may be ``NULL``.

Restore API
-----------

.. code-block:: c

   struct yk_restore_options {
       const char* new_root;      // required: working root for the restored DB
       const char* extra_config;  // optional JSON merged into recovered db_config
       size_t      xfer_size;     // reserved
   };

   yk_return_t yk_provider_restore_database(
       yk_provider_t provider,
       const char*   src_path,
       const struct yk_restore_options* options);

- ``provider``: the provider to attach the restored database to. If a
  database is already attached, it is destroyed first.
- ``src_path``: directory previously produced by ``yk_provider_snapshot_database``.
- ``options->new_root`` is **required**: it names the local directory the
  restored database will operate against. The snapshot files are first copied
  from ``src_path/data/`` into ``new_root`` and the database is then opened
  there. This keeps the snapshot on the parallel filesystem pristine and
  ensures that any subsequent writes go to local storage. Calling restore
  without ``options`` (or with ``new_root == NULL``) returns
  ``YOKAN_ERR_INVALID_ARGS``.
- ``options->extra_config``: a JSON object whose fields are merged into the
  database configuration recorded in the manifest. Useful for adjusting
  backend-specific settings (e.g. cache sizes) on restore.

Snapshot/restore example
------------------------

The following example creates a database, snapshots it without removing the
source, then restores the snapshot into a second provider and verifies that
all keys round-trip:

.. literalinclude:: ../../examples/yokan/14_snapshot/snapshot_example.c
   :language: c

Behavior and guarantees
-----------------------

**Snapshot consistency.** While ``yk_provider_snapshot_database`` runs, it
holds the same lock that database migration takes: writers are blocked for
the duration of the file copy. For large on-disk databases this can be a
non-trivial pause; plan snapshot frequency accordingly. In-memory backends
serialize the full database to a temporary file before copying, so the lock
is held for the serialize-then-copy interval.

**Source preservation.** With ``remove_source = false`` (the typical
checkpoint pattern), the database stays attached and serving the moment the
snapshot completes. With ``remove_source = true``, the behavior matches
migration: the in-memory state or on-disk files are cleared, the database
is detached, and the provider returns ``YOKAN_ERR_INVALID_DATABASE`` for
subsequent operations.

**Restore atomicity.** Restore destroys any pre-existing database on the
target provider before the new one is attached. Once it returns
``YOKAN_SUCCESS``, the new database is live and serving. If restore fails
partway through, the target provider's previous database is already gone
(the destroy step happens before recovery is attempted) — callers should
treat this as a clean-slate scenario rather than expecting rollback.

Backend compatibility
---------------------

Snapshot and restore reuse the same per-backend machinery that powers
migration:

- Snapshot uses each backend's ``startMigration()`` to obtain the file list
  and acquire the necessary locks.
- Restore uses each backend's ``recover()`` static factory to re-open the
  database from the copied files.

Backends that don't implement these (currently ``berkeleydb`` and ``null``)
return ``YOKAN_ERR_OP_UNSUPPORTED`` from snapshot. All other built-in
backends — ``map``, ``unordered_map``, ``set``, ``unordered_set``, ``array``,
``log``, ``leveldb``, ``rocksdb``, ``lmdb``, ``gdbm``, ``tkrzw``, and
``unqlite`` — support snapshot/restore.

Using snapshot/restore with Bedrock
------------------------------------

Snapshot and restore are exposed as the ``snapshot()`` and ``restore()``
overrides on the Bedrock ``yokan`` component. Bedrock's
``ProviderManager::snapshotProvider`` and ``restoreProvider`` (or the
equivalent admin RPCs) route into these.

For restore through Bedrock, pass the working directory in the JSON options
under the key ``new_root``:

.. code-block:: json

   {
       "new_root": "/local/ssd/yokan-restored",
       "extra_config": {}
   }

Comparison with migration
-------------------------

+------------------------------+----------------------+----------------------+
| Aspect                       | Migration            | Snapshot / restore   |
+==============================+======================+======================+
| Requires REMI                | Yes                  | No                   |
+------------------------------+----------------------+----------------------+
| Requires a second provider   | Yes (destination)    | No                   |
+------------------------------+----------------------+----------------------+
| Transfer mechanism           | Mercury / REMI       | Filesystem copy      |
+------------------------------+----------------------+----------------------+
| Source preserved             | No (always cleared)  | Optional             |
+------------------------------+----------------------+----------------------+
| Typical use case             | Relocation, load     | Checkpoint to PFS    |
|                              | balancing            |                      |
+------------------------------+----------------------+----------------------+

Snapshot/restore is the right tool when source and destination are reachable
through the same filesystem (compute node and shared PFS). Migration is the
right tool when shipping a database across nodes without a shared filesystem.
