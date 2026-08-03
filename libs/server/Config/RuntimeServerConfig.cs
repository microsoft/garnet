// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Garnet.common;

namespace Garnet.server
{
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
        // Number of slots needed to index every declared ServerConfigType by its underlying value.
        static readonly int TableSize = ComputeTableSize();

        // Metadata for every ServerConfigType (index == (int)ServerConfigType). Non-runtime types keep
        // the default (IsRuntime == false) and are served by the bespoke CONFIG code, not this table.
        static readonly ConfigMeta[] Meta = BuildMeta();

        // Wire name (and aliases) -> ServerConfigType, for CONFIG parameter parsing.
        static readonly Dictionary<byte[], ServerConfigType> NameToType = BuildNameLookup();
#if NET9_0_OR_GREATER
        static readonly Dictionary<byte[], ServerConfigType>.AlternateLookup<ReadOnlySpan<byte>> NameToTypeSpanLookup =
            NameToType.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif

        // All types handled by this table (settable + read-only), for CONFIG GET *.
        static readonly ServerConfigType[] runtimeTypes = BuildRuntimeTypes();

        readonly long[] values = new long[TableSize];

        // Startup options are retained only to resolve read-only parameters that derive from live server
        // state. Runtime-adjustable values are seeded into the table at construction and are thereafter
        // read exclusively through the typed accessors so a CONFIG SET is observed everywhere.
        readonly GarnetServerOptions serverOptions;

        // Owning server, through which update actions reach the live background tasks (AOF commit, expiry
        // collection/scan) to enact lifecycle changes. Null in unit tests that construct the config
        // standalone, in which case an update action simply stores the value with no lifecycle effect.
        readonly StoreWrapper owner;

        /// <summary>All configuration types handled by this table.</summary>
        public static ReadOnlySpan<ServerConfigType> RuntimeTypes => runtimeTypes;

        /// <summary>
        /// Create a runtime configuration seeded from the startup options.
        /// </summary>
        /// <param name="serverOptions">Startup options supplying the initial value of every slot.</param>
        /// <param name="owner">
        /// Owning server, through which update actions enact task lifecycle changes. May be
        /// <see langword="null"/> for a standalone configuration with no live server to act on.
        /// </param>
        public RuntimeServerConfig(GarnetServerOptions serverOptions, StoreWrapper owner = null)
        {
            ArgumentNullException.ThrowIfNull(serverOptions);
            this.serverOptions = serverOptions;
            this.owner = owner;
            Init(serverOptions);
        }

        /// <summary>
        /// Sizes the metadata and value tables from the largest declared <see cref="ServerConfigType"/>
        /// value, so no sentinel member is required and gaps in the enum remain safe to index.
        /// </summary>
        static int ComputeTableSize()
        {
            var max = 0;
            foreach (var v in Enum.GetValuesAsUnderlyingType<ServerConfigType>())
            {
                var i = (int)v;
                if (i > max) max = i;
            }
            return max + 1;
        }

        static ConfigMeta[] BuildMeta()
        {
            var m = new ConfigMeta[TableSize];

            void Set(ServerConfigType t, string name, ConfigKind kind, long min, long max, Type enumType = null,
                ConfigTimeUnit timeUnit = ConfigTimeUnit.None, bool nonPositiveIsInfinite = false,
                ConfigUpdateAction updateAction = null)
            {
                if ((kind & ConfigKind.Enum) != 0)
                    EnsureSupportedEnum(enumType);
                EnsureValidKind(name, kind, timeUnit);
                m[(int)t] = new ConfigMeta(name, kind, min, max, enumType, IsRuntime: true, ReadOnly: false,
                    timeUnit, nonPositiveIsInfinite, UpdateAction: updateAction);
            }

            void SetReadOnly(ServerConfigType t, string name, ConfigKind kind,
                Func<GarnetServerOptions, string> formatter, ConfigTimeUnit timeUnit = ConfigTimeUnit.None)
            {
                EnsureValidKind(name, kind, timeUnit);
                m[(int)t] = new ConfigMeta(name, kind, 0, 0, null, IsRuntime: true, ReadOnly: true,
                    TimeUnit: timeUnit, ReadOnlyFormatter: formatter);
            }

            // Read-only parameters: exposed through CONFIG GET (and GET *) via this table, but CONFIG SET
            // rejects them because they are constants or physical (require restart). Their CONFIG GET value
            // is computed by the per-option formatter, which reads directly from the startup
            // GarnetServerOptions (the read-only fall-through) — no runtime slot is used.
            // NOTE: slave-read-only is intentionally NOT here: it is a per-session value (READWRITE/READONLY,
            // https://redis.io/docs/latest/commands/readwrite/) and is handled directly by the CONFIG GET
            // handler, which has the calling session in scope.
            SetReadOnly(ServerConfigType.TIMEOUT, "timeout",
                ConfigKind.Int32 | ConfigKind.Seconds | ConfigKind.TimeSpan, static _ => "0", ConfigTimeUnit.Seconds);
            SetReadOnly(ServerConfigType.SAVE, "save", ConfigKind.String, static _ => "");
            SetReadOnly(ServerConfigType.APPENDONLY, "appendonly", ConfigKind.Bool,
                static o => o.EnableAOF ? "yes" : "no");
            SetReadOnly(ServerConfigType.DATABASES, "databases", ConfigKind.Int32,
                static o => o.MaxDatabases.ToString(CultureInfo.InvariantCulture));

            // Read-only non-numeric parameters resolved directly from the startup options (file paths,
            // sockets, physical toggles). These have no runtime slot and are surfaced purely for CONFIG GET.
            SetReadOnly(ServerConfigType.DIR, "dir", ConfigKind.String,
                static o => o.CheckpointBaseDirectory);
            SetReadOnly(ServerConfigType.LOGDIR, "logdir", ConfigKind.String,
                static o => o.LogDir ?? string.Empty);
            SetReadOnly(ServerConfigType.UNIXSOCKET, "unixsocket", ConfigKind.String,
                static o => o.UnixSocketPath ?? string.Empty);
            SetReadOnly(ServerConfigType.CLUSTER_ENABLED, "cluster-enabled", ConfigKind.Bool,
                static o => o.EnableCluster ? "yes" : "no");

            // Read-only AOF parameters resolved directly from the startup options. These describe the
            // physical AOF layout or startup-only toggles and require a restart to change, so CONFIG SET
            // rejects them; they have no runtime slot and are surfaced purely for CONFIG GET.
            SetReadOnly(ServerConfigType.AOF_MEMORY, "aof-memory", ConfigKind.String,
                static o => o.AofMemorySize ?? string.Empty);
            SetReadOnly(ServerConfigType.AOF_PAGE_SIZE, "aof-page-size", ConfigKind.String,
                static o => o.AofPageSize ?? string.Empty);
            SetReadOnly(ServerConfigType.AOF_SEGMENT_SIZE, "aof-segment-size", ConfigKind.String,
                static o => o.AofSegmentSize ?? string.Empty);
            SetReadOnly(ServerConfigType.AOF_PHYSICAL_SUBLOG_COUNT, "aof-physical-sublog-count", ConfigKind.Int32,
                static o => o.AofPhysicalSublogCount.ToString(CultureInfo.InvariantCulture));
            SetReadOnly(ServerConfigType.AOF_REPLAY_TASK_COUNT, "aof-replay-task-count", ConfigKind.Int32,
                static o => o.AofReplayTaskCount.ToString(CultureInfo.InvariantCulture));
            SetReadOnly(ServerConfigType.AOF_COMMIT_WAIT, "aof-commit-wait", ConfigKind.Bool,
                static o => o.WaitForCommit ? "yes" : "no");
            SetReadOnly(ServerConfigType.AOF_SIZE_LIMIT, "aof-size-limit", ConfigKind.String,
                static o => o.AofSizeLimit ?? string.Empty);
            SetReadOnly(ServerConfigType.FAST_AOF_TRUNCATE, "fast-aof-truncate", ConfigKind.Bool,
                static o => o.FastAofTruncate ? "yes" : "no");
            SetReadOnly(ServerConfigType.AOF_NULL_DEVICE, "aof-null-device", ConfigKind.Bool,
                static o => o.UseAofNullDevice ? "yes" : "no");

            Set(ServerConfigType.CLUSTER_NODE_TIMEOUT, "cluster-node-timeout",
                ConfigKind.Int32 | ConfigKind.Seconds | ConfigKind.TimeSpan, 0, int.MaxValue,
                timeUnit: ConfigTimeUnit.Seconds, nonPositiveIsInfinite: true);
            Set(ServerConfigType.REPLICA_SYNC_DELAY, "replica-sync-delay",
                ConfigKind.Int32 | ConfigKind.Milliseconds | ConfigKind.Seconds | ConfigKind.TimeSpan, 0, int.MaxValue,
                timeUnit: ConfigTimeUnit.Milliseconds);
            Set(ServerConfigType.REPLICATION_OFFSET_MAX_LAG, "replica-offset-max-lag", ConfigKind.Int32, -1, int.MaxValue);
            Set(ServerConfigType.AOF_TAIL_WITNESS_FREQ, "aof-tail-witness-freq",
                ConfigKind.Int32 | ConfigKind.Milliseconds | ConfigKind.Seconds | ConfigKind.TimeSpan, 0, int.MaxValue,
                timeUnit: ConfigTimeUnit.Milliseconds);
            Set(ServerConfigType.AOF_REPLAY_MAX_DRIFT, "aof-replay-max-drift", ConfigKind.Int64, -1, long.MaxValue);
            Set(ServerConfigType.REPL_DISKLESS_SYNC_DELAY, "repl-diskless-sync-delay",
                ConfigKind.Int32 | ConfigKind.Seconds | ConfigKind.TimeSpan, 0, int.MaxValue,
                timeUnit: ConfigTimeUnit.Seconds);
            Set(ServerConfigType.REPL_ATTACH_TIMEOUT, "repl-attach-timeout",
                ConfigKind.Int32 | ConfigKind.Seconds | ConfigKind.TimeSpan, 0, int.MaxValue,
                timeUnit: ConfigTimeUnit.Seconds, nonPositiveIsInfinite: true);
            Set(ServerConfigType.CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT, "cluster-replication-reestablishment-timeout",
                ConfigKind.Int32 | ConfigKind.Seconds | ConfigKind.TimeSpan, 0, int.MaxValue,
                timeUnit: ConfigTimeUnit.Seconds);
            Set(ServerConfigType.COMPACTION_MAX_SEGMENTS, "compaction-max-segments", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.COMPACTION_FORCE_DELETE, "compaction-force-delete", ConfigKind.Bool, 0, 1);
            Set(ServerConfigType.COMPACTION_TYPE, "compaction-type", ConfigKind.Enum, 0, 0, typeof(LogCompactionType));
            Set(ServerConfigType.SLOWLOG_LOG_SLOWER_THAN, "slowlog-log-slower-than",
                ConfigKind.Int32 | ConfigKind.Microseconds | ConfigKind.TimeSpan, 0, int.MaxValue,
                timeUnit: ConfigTimeUnit.Microseconds);
            Set(ServerConfigType.OBJECT_SCAN_COUNT_LIMIT, "object-scan-count-limit", ConfigKind.Int32, 0, int.MaxValue);
            Set(ServerConfigType.SG_GET, "sg-get", ConfigKind.Bool, 0, 1);

            // AOF size-limit enforcement frequency (seconds): the background checkpoint-enforcement task
            // re-reads this each iteration, so CONFIG SET takes effect on the running task.
            Set(ServerConfigType.AOF_SIZE_LIMIT_ENFORCE_FREQUENCY, "aof-size-limit-enforce-frequency",
                ConfigKind.Int32, 0, int.MaxValue);

            // Background-task frequencies whose change is enacted by restarting / killing the owning task
            // through the UpdateAction: the tasks capture their interval at start, so a runtime change
            // requires a kill+restart rather than a re-read.
            //
            // aof-commit-freq (ms): -1 = manual commit (no periodic task), > 0 = periodic commit interval.
            // A value of 0 (per-operation auto-commit) is baked into the AOF log at startup and cannot be
            // toggled on a live log; ApplyCommitFrequencyUpdate rejects moving to 0 and rejects any change
            // when the server started at 0.
            Set(ServerConfigType.AOF_COMMIT_FREQ, "aof-commit-freq", ConfigKind.Int32, -1, int.MaxValue,
                updateAction: ApplyCommitFrequencyUpdate);
            // expired-object-collection-freq (seconds): <= 0 = disabled (no task), > 0 = collection interval.
            Set(ServerConfigType.EXPIRED_OBJECT_COLLECTION_FREQ, "expired-object-collection-freq",
                ConfigKind.Int32, 0, int.MaxValue, updateAction: ApplyExpiredObjectCollectionUpdate);
            // expired-key-deletion-scan-freq (seconds): <= 0 = disabled (no task, on-demand EXPDELSCAN
            // allowed), > 0 = background scan interval.
            Set(ServerConfigType.EXPIRED_KEY_DELETION_SCAN_FREQ, "expired-key-deletion-scan-freq",
                ConfigKind.Int32, -1, int.MaxValue, updateAction: ApplyExpiredKeyDeletionUpdate);

            return m;
        }

        static Dictionary<byte[], ServerConfigType> BuildNameLookup()
        {
            var d = new Dictionary<byte[], ServerConfigType>(ConfigNameComparer.Instance);
            for (var i = 0; i < Meta.Length; i++)
            {
                if (Meta[i].IsRuntime)
                    d[Encoding.ASCII.GetBytes(Meta[i].Name)] = (ServerConfigType)i;
            }

            // Redis / CLI compatible alias for cluster-node-timeout.
            d["cluster-timeout"u8.ToArray()] = ServerConfigType.CLUSTER_NODE_TIMEOUT;
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
        void Init(GarnetServerOptions o)
        {
            values[(int)ServerConfigType.CLUSTER_NODE_TIMEOUT] = o.ClusterTimeout;
            values[(int)ServerConfigType.REPLICA_SYNC_DELAY] = o.ReplicaSyncDelayMs;
            values[(int)ServerConfigType.REPLICATION_OFFSET_MAX_LAG] = o.ReplicationOffsetMaxLag;
            values[(int)ServerConfigType.AOF_TAIL_WITNESS_FREQ] = o.AofTailWitnessFreqMs;
            values[(int)ServerConfigType.AOF_REPLAY_MAX_DRIFT] = o.AofReplayMaxDrift;
            values[(int)ServerConfigType.REPL_DISKLESS_SYNC_DELAY] = o.ReplicaDisklessSyncDelay;
            values[(int)ServerConfigType.REPL_ATTACH_TIMEOUT] = SecondsFromTimeSpan(o.ReplicaAttachTimeout);
            values[(int)ServerConfigType.CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT] = o.ClusterReplicationReestablishmentTimeout;
            values[(int)ServerConfigType.COMPACTION_MAX_SEGMENTS] = o.CompactionMaxSegments;
            values[(int)ServerConfigType.COMPACTION_FORCE_DELETE] = o.CompactionForceDelete ? 1 : 0;
            values[(int)ServerConfigType.COMPACTION_TYPE] = (int)o.CompactionType;
            values[(int)ServerConfigType.SLOWLOG_LOG_SLOWER_THAN] = o.SlowLogThreshold;
            values[(int)ServerConfigType.OBJECT_SCAN_COUNT_LIMIT] = o.ObjectScanCountLimit;
            values[(int)ServerConfigType.SG_GET] = o.EnableScatterGatherGet ? 1 : 0;
            values[(int)ServerConfigType.AOF_SIZE_LIMIT_ENFORCE_FREQUENCY] = o.AofSizeLimitEnforceFrequencySecs;
            values[(int)ServerConfigType.AOF_COMMIT_FREQ] = o.CommitFrequencyMs;
            values[(int)ServerConfigType.EXPIRED_OBJECT_COLLECTION_FREQ] = o.ExpiredObjectCollectionFrequencySecs;
            values[(int)ServerConfigType.EXPIRED_KEY_DELETION_SCAN_FREQ] = o.ExpiredKeyDeletionScanFrequencySecs;
        }

        /// <summary>Current value of <paramref name="type"/> as a 32-bit integer, in its native unit.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetInt(ServerConfigType type)
        {
            AssertKind(type, ConfigKind.Int32);
            return (int)Volatile.Read(ref values[(int)type]);
        }

        /// <summary>Current value of <paramref name="type"/> as a 64-bit integer, in its native unit.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLong(ServerConfigType type)
        {
            AssertKind(type, ConfigKind.Int64);
            return Volatile.Read(ref values[(int)type]);
        }

        /// <summary>Current value of <paramref name="type"/> as a boolean.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetBool(ServerConfigType type)
        {
            AssertKind(type, ConfigKind.Bool);
            return Volatile.Read(ref values[(int)type]) != 0;
        }

        /// <summary>Current value of <paramref name="type"/> converted to microseconds.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetMicroseconds(ServerConfigType type)
            => ConvertDuration(type, ConfigKind.Microseconds, ConfigTimeUnit.Microseconds);

        /// <summary>Current value of <paramref name="type"/> converted to milliseconds.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetMilliseconds(ServerConfigType type)
            => ConvertDuration(type, ConfigKind.Milliseconds, ConfigTimeUnit.Milliseconds);

        /// <summary>Current value of <paramref name="type"/> converted to seconds.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSeconds(ServerConfigType type)
            => ConvertDuration(type, ConfigKind.Seconds, ConfigTimeUnit.Seconds);

        /// <summary>
        /// Current value of <paramref name="type"/> as a <see cref="TimeSpan"/>. For options whose
        /// non-positive value denotes no timeout, returns <see cref="Timeout.InfiniteTimeSpan"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TimeSpan GetTimeSpan(ServerConfigType type)
        {
            AssertKind(type, ConfigKind.TimeSpan);

            ref readonly var meta = ref Meta[(int)type];
            var raw = Volatile.Read(ref values[(int)type]);
            if (raw <= 0 && meta.NonPositiveIsInfinite)
                return Timeout.InfiniteTimeSpan;

            return meta.TimeUnit switch
            {
                ConfigTimeUnit.Microseconds => TimeSpan.FromTicks(raw * (TimeSpan.TicksPerMillisecond / 1000)),
                ConfigTimeUnit.Milliseconds => TimeSpan.FromMilliseconds(raw),
                _ => TimeSpan.FromSeconds(raw),
            };
        }

        /// <summary>
        /// Current value of <paramref name="type"/> as <typeparamref name="TEnum"/>.
        /// </summary>
        /// <typeparam name="TEnum">Enum type the option is declared as.</typeparam>
        /// <param name="type">Configuration parameter to read.</param>
        /// <returns>The current value as a declared member of <typeparamref name="TEnum"/>.</returns>
        /// <exception cref="InvalidOperationException">
        /// The slot does not hold a declared member of <typeparamref name="TEnum"/>. Unreachable in
        /// practice: every write goes through <see cref="TrySet"/>, which rejects undeclared values.
        /// Unlike the debug-only assertions above, this check is retained in release builds so a corrupt
        /// slot surfaces as a fault rather than an out-of-range enum flowing into the server.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TEnum GetEnum<TEnum>(ServerConfigType type) where TEnum : unmanaged, Enum
        {
            AssertKind(type, ConfigKind.Enum);
            Debug.Assert(Meta[(int)type].EnumType == typeof(TEnum),
                $"Configuration '{type}' is declared as '{Meta[(int)type].EnumType}' and cannot be read as '{typeof(TEnum)}'.");

            // The slot holds the underlying value widened to 64 bits (long)
            var raw = Volatile.Read(ref values[(int)type]);

            // Use the extension method to validate boundaries, check definition, and safely narrow/cast
            if (!raw.TryParseToEnum(out TEnum enumValue))
            {
                throw new InvalidOperationException(
                    $"The raw configuration value {raw} is out of bounds or not a defined member of enum {typeof(TEnum).Name}.");
            }

            return enumValue;
        }

        /// <summary>
        /// Validate <paramref name="value"/> and, if valid, update the slot for <paramref name="type"/>.
        /// </summary>
        /// <param name="type">Configuration parameter to update.</param>
        /// <param name="value">Value as supplied on the CONFIG SET wire.</param>
        /// <param name="error">Error message prefixed with "ERR " when the value is rejected.</param>
        /// <returns><see langword="true"/> if the slot was updated.</returns>
        public bool TrySet(ServerConfigType type, string value, out string error)
        {
            error = null;
            ref readonly var meta = ref Meta[(int)type];
            if (meta.ReadOnly)
            {
                error = $"ERR Option '{meta.Name}' is read-only and cannot be set at runtime.";
                return false;
            }

            long parsed;
            switch (meta.Kind & ConfigKind.StorageMask)
            {
                case ConfigKind.Int32:
                    {
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i32))
                        {
                            error = $"ERR Invalid value for '{meta.Name}': expected an integer.";
                            return false;
                        }
                        // A non-positive value denotes an infinite timeout; normalize so CONFIG GET never
                        // reports a negative value that would be silently reinterpreted as infinite.
                        if (i32 < 0 && meta.NonPositiveIsInfinite)
                            i32 = 0;
                        if (i32 < meta.Min || i32 > meta.Max)
                        {
                            error = $"ERR Value for '{meta.Name}' is out of range ({meta.Min}..{meta.Max}).";
                            return false;
                        }
                        parsed = i32;
                        break;
                    }
                case ConfigKind.Int64:
                    {
                        if (!long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i64))
                        {
                            error = $"ERR Invalid value for '{meta.Name}': expected an integer.";
                            return false;
                        }
                        if (i64 < 0 && meta.NonPositiveIsInfinite)
                            i64 = 0;
                        if (i64 < meta.Min || i64 > meta.Max)
                        {
                            error = $"ERR Value for '{meta.Name}' is out of range ({meta.Min}..{meta.Max}).";
                            return false;
                        }
                        parsed = i64;
                        break;
                    }
                case ConfigKind.Bool:
                    {
                        switch (value.ToLowerInvariant())
                        {
                            case "yes":
                            case "true":
                            case "1":
                                parsed = 1;
                                break;
                            case "no":
                            case "false":
                            case "0":
                                parsed = 0;
                                break;
                            default:
                                error = $"ERR Invalid value for '{meta.Name}': expected 'yes' or 'no'.";
                                return false;
                        }
                        break;
                    }
                case ConfigKind.Enum:
                    {
                        if (!value.TryParseEnumToLong(meta.EnumType, out var enumValue))
                        {
                            error = $"ERR Invalid value for '{meta.Name}': '{value}'.";
                            return false;
                        }
                        parsed = enumValue;
                        break;
                    }
                default:
                    error = $"ERR Option '{meta.Name}' is not runtime-adjustable.";
                    return false;
            }

            // Publish the new value first so any task an update action restarts observes it, then run the
            // action. On rejection, roll the slot back so the option is left unchanged.
            var oldValue = Volatile.Read(ref values[(int)type]);
            Volatile.Write(ref values[(int)type], parsed);

            if (meta.UpdateAction != null && !meta.UpdateAction(this, oldValue, parsed, out error))
            {
                Volatile.Write(ref values[(int)type], oldValue);
                return false;
            }

            return true;
        }

        // Enacts an aof-commit-freq change on the periodic AOF commit task. A value of 0 (per-operation
        // auto-commit) is fixed into the AOF log at construction and cannot be toggled on a live log, so a
        // move to 0 — or any change when the server started at 0 — is rejected. For the safe {-1, >0}
        // transitions the commit task is restarted (or killed) to pick up the new interval.
        static bool ApplyCommitFrequencyUpdate(RuntimeServerConfig config, long oldValue, long newValue, out string error)
        {
            error = null;
            if (newValue == 0)
            {
                error = "ERR 'aof-commit-freq' cannot be set to 0 at runtime; per-operation auto-commit is fixed at startup.";
                return false;
            }
            if (config.serverOptions.CommitFrequencyMs == 0)
            {
                error = "ERR 'aof-commit-freq' cannot be changed at runtime because the server started with per-operation auto-commit (0).";
                return false;
            }
            config.owner?.ReconcilePrimaryTask(TaskType.CommitTask);
            return true;
        }

        // Enacts an expired-object-collection-freq change by restarting / killing the collection task so it
        // picks up the new interval (or stops when disabled).
        static bool ApplyExpiredObjectCollectionUpdate(RuntimeServerConfig config, long oldValue, long newValue, out string error)
        {
            error = null;
            config.owner?.ReconcilePrimaryTask(TaskType.ObjectCollectTask);
            return true;
        }

        // Enacts an expired-key-deletion-scan-freq change by restarting / killing the scan task so it picks
        // up the new interval (or stops, re-enabling on-demand EXPDELSCAN, when disabled).
        static bool ApplyExpiredKeyDeletionUpdate(RuntimeServerConfig config, long oldValue, long newValue, out string error)
        {
            error = null;
            config.owner?.ReconcilePrimaryTask(TaskType.ExpiredKeyDeletionTask);
            return true;
        }

        /// <summary>Canonical wire name of <paramref name="type"/>.</summary>
        public static string Name(ServerConfigType type) => Meta[(int)type].Name;

        /// <summary>Resolve a config parameter name (honoring aliases) to a config type handled by this table.</summary>
        public static bool TryGetType(ReadOnlySpan<byte> name, out ServerConfigType type)
        {
#if NET9_0_OR_GREATER
            return NameToTypeSpanLookup.TryGetValue(name, out type);
#else
            return NameToType.TryGetValue(name.ToArray(), out type);
#endif
        }

        // The CLI/config surface expresses these timeouts in seconds and treats <= 0 as an infinite timeout.
        static int SecondsFromTimeSpan(TimeSpan ts)
            => ts == Timeout.InfiniteTimeSpan || ts < TimeSpan.Zero ? 0 : (int)ts.TotalSeconds;

        [Conditional("DEBUG")]
        static void AssertKind(ServerConfigType type, ConfigKind requestedKind)
        {
            Debug.Assert((Meta[(int)type].Kind & requestedKind) != 0,
                $"Configuration '{type}' is declared as {Meta[(int)type].Kind} and cannot be read as {requestedKind}.");
        }

        // Read a duration-valued slot and convert it from its stored unit to the requested unit.
        // Conversion to a coarser unit truncates; use GetTimeSpan when full precision is required.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long ConvertDuration(ServerConfigType type, ConfigKind requestedKind, ConfigTimeUnit requestedUnit)
        {
            AssertKind(type, requestedKind);

            ref readonly var meta = ref Meta[(int)type];
            var raw = Volatile.Read(ref values[(int)type]);
            if (meta.TimeUnit == requestedUnit)
                return raw;

            var storedMicros = meta.TimeUnit switch
            {
                ConfigTimeUnit.Microseconds => raw,
                ConfigTimeUnit.Milliseconds => raw * 1000,
                _ => raw * 1000_000,
            };

            return requestedUnit switch
            {
                ConfigTimeUnit.Microseconds => storedMicros,
                ConfigTimeUnit.Milliseconds => storedMicros / 1000,
                _ => storedMicros / 1000_000,
            };
        }

        static void EnsureValidKind(string name, ConfigKind kind, ConfigTimeUnit timeUnit)
        {
            var storageKind = kind & ConfigKind.StorageMask;
            if (storageKind == ConfigKind.None || (storageKind & (storageKind - 1)) != 0)
                throw new InvalidOperationException(
                    $"Configuration '{name}' must declare exactly one storage kind, but declares '{storageKind}'.");

            if ((kind & ConfigKind.DurationMask) != 0 && timeUnit == ConfigTimeUnit.None)
                throw new InvalidOperationException(
                    $"Configuration '{name}' declares duration views but no time unit.");

            if ((kind & ConfigKind.DurationMask) == 0 && timeUnit != ConfigTimeUnit.None)
                throw new InvalidOperationException(
                    $"Configuration '{name}' declares a time unit but no duration views.");
        }

        static void EnsureSupportedEnum(Type enumType)
        {
            if (enumType == null || !enumType.IsEnum)
                throw new InvalidOperationException(
                    $"Runtime configuration option declares '{enumType}', which is not an enum type.");

            // Every value is widened into the 64-bit slot, so any integral backing type is supported.
            // The guard exists to reject a backing type that could not round-trip through the slot.
            switch (Type.GetTypeCode(Enum.GetUnderlyingType(enumType)))
            {
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return;
                default:
                    throw new InvalidOperationException(
                        $"Enum '{enumType}' is backed by '{Enum.GetUnderlyingType(enumType)}', which is not supported.");
            }
        }

        /// <summary>Current value of <paramref name="type"/> as its RESP string representation.</summary>
        public string RespFormat(ServerConfigType type)
        {
            ref readonly var meta = ref Meta[(int)type];
            if (meta.ReadOnly)
                // Read-only fall-through: the value comes straight from the live startup options.
                return meta.ReadOnlyFormatter?.Invoke(serverOptions) ?? string.Empty;

            var raw = Volatile.Read(ref values[(int)type]);
            return (meta.Kind & ConfigKind.StorageMask) switch
            {
                ConfigKind.Int32 => ((int)raw).ToString(CultureInfo.InvariantCulture),
                ConfigKind.Int64 => raw.ToString(CultureInfo.InvariantCulture),
                ConfigKind.Bool => raw != 0 ? "yes" : "no",
                ConfigKind.Enum => Enum.GetName(meta.EnumType, raw.ToEnumLiteral(Enum.GetUnderlyingType(meta.EnumType)))
                    ?? raw.ToString(CultureInfo.InvariantCulture),
                _ => raw.ToString(CultureInfo.InvariantCulture),
            };
        }
    }
}