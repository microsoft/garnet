// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public enum ServerConfigType : int
    {
        NONE,
        ALL,
        TIMEOUT,
        SAVE,
        APPENDONLY,
        SLAVE_READ_ONLY,
        DATABASES,

        // Runtime-adjustable options (read live at their point of use, no physical change to the
        // running server). Backed by the long[] table in RuntimeServerConfig, seeded from
        // GarnetServerOptions at startup and updated via CONFIG SET.
        // The unit of a time-based option is declared as metadata in RuntimeServerConfig, which exposes
        // the value through every unit it supports, so it is not encoded in the member name.
        CLUSTER_NODE_TIMEOUT,
        REPLICA_SYNC_DELAY,
        REPLICATION_OFFSET_MAX_LAG,
        AOF_TAIL_WITNESS_FREQ,
        AOF_REPLAY_MAX_DRIFT,
        REPL_DISKLESS_SYNC_DELAY,
        REPL_ATTACH_TIMEOUT,
        CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT,
        COMPACTION_MAX_SEGMENTS,
        COMPACTION_FORCE_DELETE,
        COMPACTION_TYPE,
        SLOWLOG_LOG_SLOWER_THAN,
        OBJECT_SCAN_COUNT_LIMIT,
        SG_GET,

        // Read-only, non-numeric parameters (file paths, sockets, physical toggles). Exposed through
        // CONFIG GET via the read-only fall-through — their value is read directly from the startup
        // GarnetServerOptions and CONFIG SET rejects them. They have no backing long[] slot.
        DIR,
        LOGDIR,
        UNIXSOCKET,
        CLUSTER_ENABLED,
    }
}