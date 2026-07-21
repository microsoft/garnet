// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Kind of value stored in a runtime configuration slot. A slot is a raw 8-byte cell; the kind
    /// determines how it is parsed (CONFIG SET), validated, formatted (CONFIG GET) and read back.
    /// </summary>
    internal enum ConfigKind : byte
    {
        Int32,
        Int64,
        Bool,
        Enum,
        Seconds,
        String,
    }

    /// <summary>
    /// Central table of runtime-adjustable server configuration values, indexed by
    /// <see cref="ServerConfigType"/>. Values are seeded from <see cref="GarnetServerOptions"/> at
    /// startup and may be updated at runtime through CONFIG SET. Held by
    /// <see cref="StoreWrapper"/> so the live value is reachable across the server and cluster layers.
    /// <para/>
    /// Backed by a single <see cref="long"/>[] (allocation-free, contiguous, O(1) indexed,
    /// <see cref="Volatile"/> access is atomic on 64-bit). Each slot is a raw 8-byte cell whose
    /// interpretation is given by the per-option <see cref="ConfigMeta"/> — heterogeneous types
    /// (int, long, bool, enum, seconds-based timeout) are losslessly encoded into the slot.
    /// </summary>
    public sealed class RuntimeServerConfig
    {
        internal readonly record struct ConfigMeta(
            string Name,
            ConfigKind Kind,
            long Min,
            long Max,
            Type EnumType,
            bool IsRuntime,
            bool ReadOnly);

        // Metadata for every ServerConfigType (index == (int)ServerConfigType). Non-runtime types keep
        // the default (IsRuntime == false) and are served by the bespoke CONFIG code, not this table.
        static readonly ConfigMeta[] Meta = BuildMeta();

        // Wire name (and aliases) -> ServerConfigType, for CONFIG parameter parsing.
        static readonly Dictionary<string, ServerConfigType> NameToType = BuildNameLookup();

        // All types handled by this table (settable + read-only), for CONFIG GET *.
        static readonly ServerConfigType[] runtimeTypes = BuildRuntimeTypes();

        readonly long[] values = new long[(int)ServerConfigType.COUNT];

        // Owning StoreWrapper, used to compute read-only values that derive from live server state
        // (e.g. appendonly, databases, slave-read-only). Set once via SetOwner after construction.
        StoreWrapper owner;

        /// <summary>All configuration types handled by this table.</summary>
        public static ReadOnlySpan<ServerConfigType> RuntimeTypes => runtimeTypes;

        static ConfigMeta[] BuildMeta()
        {
            var m = new ConfigMeta[(int)ServerConfigType.COUNT];

            void Set(ServerConfigType t, string name, ConfigKind kind, long min, long max, Type enumType = null)
                => m[(int)t] = new ConfigMeta(name, kind, min, max, enumType, IsRuntime: true, ReadOnly: false);

            void SetReadOnly(ServerConfigType t, string name, ConfigKind kind)
                => m[(int)t] = new ConfigMeta(name, kind, 0, 0, null, IsRuntime: true, ReadOnly: true);

            // Read-only parameters: exposed through CONFIG GET (and GET *) via this table, but CONFIG SET
            // rejects them because they are constants or physical (require restart). Their values are
            // computed in FormatReadOnly from the owning StoreWrapper.
            // NOTE: slave-read-only is intentionally NOT here: it is a per-session value (READWRITE/READONLY,
            // https://redis.io/docs/latest/commands/readwrite/) and is handled directly by the CONFIG GET
            // handler, which has the calling session in scope.
            SetReadOnly(ServerConfigType.TIMEOUT_SECONDS, "timeout", ConfigKind.Int32);
            SetReadOnly(ServerConfigType.SAVE, "save", ConfigKind.String);
            SetReadOnly(ServerConfigType.APPENDONLY, "appendonly", ConfigKind.Bool);
            SetReadOnly(ServerConfigType.DATABASES, "databases", ConfigKind.Int32);

            Set(ServerConfigType.CLUSTER_NODE_TIMEOUT_SECONDS, "cluster-node-timeout", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.REPLICA_SYNC_DELAY_MS, "replica-sync-delay", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.REPLICATION_OFFSET_MAX_LAG, "replica-offset-max-lag", ConfigKind.Int32, -1, int.MaxValue);
            Set(ServerConfigType.AOF_TAIL_WITNESS_FREQ_MS, "aof-tail-witness-freq", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.AOF_REPLAY_MAX_DRIFT, "aof-replay-max-drift", ConfigKind.Int64, -1, long.MaxValue);
            Set(ServerConfigType.REPL_DISKLESS_SYNC_DELAY_SECONDS, "repl-diskless-sync-delay", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.REPL_ATTACH_TIMEOUT_SECONDS, "repl-attach-timeout", ConfigKind.Seconds, 0, int.MaxValue);
            Set(ServerConfigType.CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT_SECONDS, "cluster-replication-reestablishment-timeout", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.COMPACTION_MAX_SEGMENTS, "compaction-max-segments", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.COMPACTION_FORCE_DELETE, "compaction-force-delete", ConfigKind.Bool, 0, 1);
            Set(ServerConfigType.COMPACTION_TYPE, "compaction-type", ConfigKind.Enum, 0, 0, typeof(LogCompactionType));
            Set(ServerConfigType.SLOWLOG_LOG_SLOWER_THAN_MICROS, "slowlog-log-slower-than", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.OBJECT_SCAN_COUNT_LIMIT, "object-scan-count-limit", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.SG_GET, "sg-get", ConfigKind.Bool, 0, 1);

            return m;
        }

        static Dictionary<string, ServerConfigType> BuildNameLookup()
        {
            var d = new Dictionary<string, ServerConfigType>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < Meta.Length; i++)
            {
                if (Meta[i].IsRuntime)
                    d[Meta[i].Name] = (ServerConfigType)i;
            }

            // Redis / CLI compatible alias for cluster-node-timeout.
            d["cluster-timeout"] = ServerConfigType.CLUSTER_NODE_TIMEOUT_SECONDS;
            return d;
        }

        static ServerConfigType[] BuildRuntimeTypes()
        {
            var list = new List<ServerConfigType>();
            for (var i = 0; i < Meta.Length; i++)
            {
                if (Meta[i].IsRuntime)
                    list.Add((ServerConfigType)i);
            }
            return [.. list];
        }

        /// <summary>Seed every runtime slot from the startup options.</summary>
        public void Init(GarnetServerOptions o)
        {
            values[(int)ServerConfigType.CLUSTER_NODE_TIMEOUT_SECONDS] = o.ClusterTimeout;
            values[(int)ServerConfigType.REPLICA_SYNC_DELAY_MS] = o.ReplicaSyncDelayMs;
            values[(int)ServerConfigType.REPLICATION_OFFSET_MAX_LAG] = o.ReplicationOffsetMaxLag;
            values[(int)ServerConfigType.AOF_TAIL_WITNESS_FREQ_MS] = o.AofTailWitnessFreqMs;
            values[(int)ServerConfigType.AOF_REPLAY_MAX_DRIFT] = o.AofReplayMaxDrift;
            values[(int)ServerConfigType.REPL_DISKLESS_SYNC_DELAY_SECONDS] = o.ReplicaDisklessSyncDelay;
            values[(int)ServerConfigType.REPL_ATTACH_TIMEOUT_SECONDS] = SecondsFromTimeSpan(o.ReplicaAttachTimeout);
            values[(int)ServerConfigType.CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT_SECONDS] = o.ClusterReplicationReestablishmentTimeout;
            values[(int)ServerConfigType.COMPACTION_MAX_SEGMENTS] = o.CompactionMaxSegments;
            values[(int)ServerConfigType.COMPACTION_FORCE_DELETE] = o.CompactionForceDelete ? 1 : 0;
            values[(int)ServerConfigType.COMPACTION_TYPE] = (int)o.CompactionType;
            values[(int)ServerConfigType.SLOWLOG_LOG_SLOWER_THAN_MICROS] = o.SlowLogThreshold;
            values[(int)ServerConfigType.OBJECT_SCAN_COUNT_LIMIT] = o.ObjectScanCountLimit;
            values[(int)ServerConfigType.SG_GET] = o.EnableScatterGatherGet ? 1 : 0;
        }

        /// <summary>
        /// Set the owning <see cref="StoreWrapper"/>, used to compute read-only values that derive from
        /// live server state. Called once by the owner after construction.
        /// </summary>
        public void SetOwner(StoreWrapper storeWrapper) => owner = storeWrapper;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetInt(ServerConfigType type) => (int)Volatile.Read(ref values[(int)type]);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLong(ServerConfigType type) => Volatile.Read(ref values[(int)type]);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetBool(ServerConfigType type) => Volatile.Read(ref values[(int)type]) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TimeSpan GetTimeSpan(ServerConfigType type)
        {
            var seconds = Volatile.Read(ref values[(int)type]);
            return seconds <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(seconds);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TEnum GetEnum<TEnum>(ServerConfigType type) where TEnum : unmanaged, Enum
        {
            var value = (int)Volatile.Read(ref values[(int)type]);
            return Unsafe.As<int, TEnum>(ref value);
        }

        /// <summary>
        /// Validate <paramref name="value"/> and, if valid, update the slot for <paramref name="type"/>.
        /// Returns <see langword="null"/> on success, otherwise an error message prefixed with "ERR ".
        /// </summary>
        public string TrySet(ServerConfigType type, string value)
        {
            ref readonly var meta = ref Meta[(int)type];
            if (meta.ReadOnly)
                return $"ERR Option '{meta.Name}' is read-only and cannot be set at runtime.";
            switch (meta.Kind)
            {
                case ConfigKind.Int32:
                    {
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                            return $"ERR Invalid value for '{meta.Name}': expected an integer.";
                        if (parsed < meta.Min || parsed > meta.Max)
                            return $"ERR Value for '{meta.Name}' is out of range ({meta.Min}..{meta.Max}).";
                        Volatile.Write(ref values[(int)type], parsed);
                        return null;
                    }
                case ConfigKind.Int64:
                    {
                        if (!long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                            return $"ERR Invalid value for '{meta.Name}': expected an integer.";
                        if (parsed < meta.Min || parsed > meta.Max)
                            return $"ERR Value for '{meta.Name}' is out of range ({meta.Min}..{meta.Max}).";
                        Volatile.Write(ref values[(int)type], parsed);
                        return null;
                    }
                case ConfigKind.Bool:
                    {
                        switch (value.ToLowerInvariant())
                        {
                            case "yes":
                            case "true":
                            case "1":
                                Volatile.Write(ref values[(int)type], 1);
                                return null;
                            case "no":
                            case "false":
                            case "0":
                                Volatile.Write(ref values[(int)type], 0);
                                return null;
                            default:
                                return $"ERR Invalid value for '{meta.Name}': expected 'yes' or 'no'.";
                        }
                    }
                case ConfigKind.Enum:
                    {
                        if (!Enum.TryParse(meta.EnumType, value, ignoreCase: true, out var parsed) ||
                            !Enum.IsDefined(meta.EnumType, parsed))
                            return $"ERR Invalid value for '{meta.Name}': '{value}'.";
                        Volatile.Write(ref values[(int)type], Convert.ToInt32(parsed, CultureInfo.InvariantCulture));
                        return null;
                    }
                case ConfigKind.Seconds:
                    {
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds))
                            return $"ERR Invalid value for '{meta.Name}': expected an integer number of seconds.";
                        // <= 0 is stored as-is and interpreted as an infinite timeout on read.
                        Volatile.Write(ref values[(int)type], seconds);
                        return null;
                    }
                default:
                    return $"ERR Option '{meta.Name}' is not runtime-adjustable.";
            }
        }

        /// <summary>Current value of <paramref name="type"/> as its RESP string representation.</summary>
        public string Format(ServerConfigType type)
        {
            ref readonly var meta = ref Meta[(int)type];
            if (meta.ReadOnly)
                return FormatReadOnly(type);

            var raw = Volatile.Read(ref values[(int)type]);
            return meta.Kind switch
            {
                ConfigKind.Int32 => ((int)raw).ToString(CultureInfo.InvariantCulture),
                ConfigKind.Int64 => raw.ToString(CultureInfo.InvariantCulture),
                ConfigKind.Bool => raw != 0 ? "yes" : "no",
                ConfigKind.Seconds => ((int)raw).ToString(CultureInfo.InvariantCulture),
                ConfigKind.Enum => Enum.GetName(meta.EnumType, (int)raw) ?? ((int)raw).ToString(CultureInfo.InvariantCulture),
                _ => raw.ToString(CultureInfo.InvariantCulture),
            };
        }

        // Compute the value of a read-only parameter from live server state. These are server-wide values.
        string FormatReadOnly(ServerConfigType type) => type switch
        {
            ServerConfigType.TIMEOUT_SECONDS => "0",
            ServerConfigType.SAVE => "",
            ServerConfigType.APPENDONLY => owner.serverOptions.EnableAOF ? "yes" : "no",
            ServerConfigType.DATABASES => owner.serverOptions.MaxDatabases.ToString(CultureInfo.InvariantCulture),
            _ => "",
        };

        /// <summary>Canonical wire name of <paramref name="type"/>.</summary>
        public static string Name(ServerConfigType type) => Meta[(int)type].Name;

        /// <summary>Resolve a config parameter name (honoring aliases) to a config type handled by this table.</summary>
        public static bool TryGetType(ReadOnlySpan<byte> name, out ServerConfigType type)
            => NameToType.TryGetValue(Encoding.ASCII.GetString(name), out type);

        // The CLI/config surface expresses these timeouts in seconds and treats <= 0 as an infinite timeout.
        static int SecondsFromTimeSpan(TimeSpan ts)
            => ts == Timeout.InfiniteTimeSpan || ts < TimeSpan.Zero ? 0 : (int)ts.TotalSeconds;
    }
}