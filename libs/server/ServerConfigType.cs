// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public enum ServerConfigType : int
    {
        NONE,
        ALL,
        TIMEOUT_SECONDS,
        SAVE,
        APPENDONLY,
        SLAVE_READ_ONLY,
        DATABASES,

        // Runtime-adjustable options (read live at their point of use, no physical change to the
        // running server). Backed by the long[] table in RuntimeServerConfig, seeded from
        // GarnetServerOptions at startup and updated via CONFIG SET.
        // Time-based options carry a unit suffix (_MS / _SECONDS / _MICROS) matching the underlying
        // GarnetServerOptions property so the unit is unambiguous at every use site.
        CLUSTER_NODE_TIMEOUT_SECONDS,
        REPLICA_SYNC_DELAY_MS,
        REPLICATION_OFFSET_MAX_LAG,
        AOF_TAIL_WITNESS_FREQ_MS,
        AOF_REPLAY_MAX_DRIFT,
        REPL_DISKLESS_SYNC_DELAY_SECONDS,
        REPL_ATTACH_TIMEOUT_SECONDS,
        CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT_SECONDS,
        COMPACTION_MAX_SEGMENTS,
        COMPACTION_FORCE_DELETE,
        COMPACTION_TYPE,
        SLOWLOG_LOG_SLOWER_THAN_MICROS,
        OBJECT_SCAN_COUNT_LIMIT,
        SG_GET,

        // Sentinel: number of ServerConfigType values, used to size the runtime value table.
        COUNT
    }
}