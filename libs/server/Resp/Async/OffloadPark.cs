// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// When a command has to run on an <see cref="OffloadExecutor"/> thread rather than the thread that received it.
        /// </summary>
        enum OffloadClass : byte
        {
            /// <summary>
            /// Runs where it is received: it does not wait on a thread, or it parks the session for whatever it waits on.
            /// </summary>
            Never,

            /// <summary>
            /// Runs on the executor when the store has evicted records to its storage tier, because it then completes a
            /// device read, or a scan over the device, on the thread it runs on.
            /// </summary>
            WhenSpilled,

            /// <summary>
            /// Always runs on the executor: it does file IO, or waits on other sessions, on the thread it runs on.
            /// </summary>
            Always,

            /// <summary>
            /// Runs on the executor when <see cref="MustOffloadConditional"/> says so.
            /// </summary>
            Conditional,
        }

        /// <summary>
        /// The <see cref="OffloadClass"/> of each command, indexed by the command.
        /// </summary>
        static readonly OffloadClass[] OffloadClasses = CreateOffloadClasses();

        /// <summary>
        /// Set by <see cref="OffloadAwaitable"/> while the session suspends to be run on the executor, and read by
        /// <see cref="ParkSuspension"/> once the session has parked: 0 for none, 1 to run it on the executor, 2 to run it
        /// there even if every executor thread is busy.
        /// </summary>
        byte deferredOffload;

        static OffloadClass[] CreateOffloadClasses()
        {
            var classes = new OffloadClass[(int)RespCommandExtensions.LastValidCommand + 1];

            // A data command can need a record the store has evicted to its storage tier
            for (var cmd = RespCommandExtensions.FirstDataCommand; cmd <= RespCommandExtensions.LastDataCommand; cmd++)
                classes[(int)cmd] = OffloadClass.WhenSpilled;

            foreach (var cmd in (ReadOnlySpan<RespCommand>)[
                RespCommand.CustomTxn, RespCommand.CustomRawStringCmd, RespCommand.CustomObjCmd, RespCommand.CustomProcedure,
                RespCommand.RUNTXP,
                RespCommand.CLUSTER_COUNTKEYSINSLOT, RespCommand.CLUSTER_GETKEYSINSLOT,
                RespCommand.CLUSTER_DELKEYSINSLOT, RespCommand.CLUSTER_DELKEYSINSLOTRANGE,
                RespCommand.CLUSTER_MIGRATE, RespCommand.CLUSTER_SYNC])
                classes[(int)cmd] = OffloadClass.WhenSpilled;

            // These park the session on what they wait for, a device read included, or touch no stored data at all
            foreach (var cmd in (ReadOnlySpan<RespCommand>)[
                RespCommand.GET, RespCommand.MGET,
                RespCommand.EXISTS, RespCommand.TYPE, RespCommand.TTL, RespCommand.PTTL,
                RespCommand.EXPIRE, RespCommand.PEXPIRE, RespCommand.EXPIREAT, RespCommand.PEXPIREAT,
                RespCommand.EXPIRETIME, RespCommand.PEXPIRETIME, RespCommand.PERSIST, RespCommand.MEMORY_USAGE,
                RespCommand.OBJECT_ENCODING, RespCommand.OBJECT_FREQ, RespCommand.OBJECT_IDLETIME, RespCommand.OBJECT_REFCOUNT,
                RespCommand.ZCARD, RespCommand.ZSCORE, RespCommand.ZMSCORE, RespCommand.ZCOUNT, RespCommand.ZRANK,
                RespCommand.ZREVRANK, RespCommand.ZRANGE, RespCommand.ZRANGEBYLEX, RespCommand.ZRANGEBYSCORE,
                RespCommand.ZREVRANGE, RespCommand.ZREVRANGEBYLEX, RespCommand.ZREVRANGEBYSCORE, RespCommand.ZRANDMEMBER,
                RespCommand.ZTTL, RespCommand.ZPTTL, RespCommand.ZEXPIRETIME, RespCommand.ZPEXPIRETIME,
                RespCommand.GEOHASH, RespCommand.GEODIST, RespCommand.GEOPOS,
                RespCommand.LPOS, RespCommand.LLEN, RespCommand.LRANGE, RespCommand.LINDEX,
                RespCommand.HGET, RespCommand.HMGET, RespCommand.HGETALL, RespCommand.HLEN, RespCommand.HSTRLEN,
                RespCommand.HEXISTS, RespCommand.HKEYS, RespCommand.HVALS, RespCommand.HTTL, RespCommand.HPTTL,
                RespCommand.HEXPIRETIME, RespCommand.HPEXPIRETIME, RespCommand.HRANDFIELD,
                RespCommand.SMEMBERS, RespCommand.SISMEMBER, RespCommand.SMISMEMBER, RespCommand.SCARD, RespCommand.SRANDMEMBER,
                RespCommand.WATCH, RespCommand.WATCHMS, RespCommand.WATCHOS, RespCommand.SPUBLISH, RespCommand.SSUBSCRIBE,
                RespCommand.MIGRATE])
                classes[(int)cmd] = OffloadClass.Never;

            // Blind writes: an upsert or a delete adds a record without reading the one it replaces
            foreach (var cmd in (ReadOnlySpan<RespCommand>)[
                RespCommand.SET, RespCommand.MSET, RespCommand.SETEX, RespCommand.PSETEX, RespCommand.DEL, RespCommand.UNLINK])
                classes[(int)cmd] = OffloadClass.Never;

            foreach (var cmd in (ReadOnlySpan<RespCommand>)[
                RespCommand.FLUSHDB, RespCommand.FLUSHALL, RespCommand.SWAPDB, RespCommand.EXPDELSCAN,
                RespCommand.RICREATE, RespCommand.RIDEL, RespCommand.RIPROMOTE, RespCommand.RIRESTORE, RespCommand.RISET,
                RespCommand.RICONFIG, RespCommand.RIEXISTS, RespCommand.RIGET, RespCommand.RIMETRICS, RespCommand.RIRANGE,
                RespCommand.RISCAN,
                RespCommand.VADD, RespCommand.VREM, RespCommand.VSETATTR, RespCommand.VCARD, RespCommand.VDIM, RespCommand.VEMB,
                RespCommand.VGETATTR, RespCommand.VINFO, RespCommand.VISMEMBER, RespCommand.VLINKS, RespCommand.VRANDMEMBER,
                RespCommand.VSIM,
                RespCommand.CONFIG_REWRITE, RespCommand.ACL_LOAD, RespCommand.ACL_SAVE,
                RespCommand.MODULE_LOADCS, RespCommand.REGISTERCS,
                RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT, RespCommand.CLUSTER_SEND_CKPT_METADATA,
                RespCommand.CLUSTER_SNAPSHOT_DATA, RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER, RespCommand.CLUSTER_ATTACH_SYNC,
                RespCommand.CLUSTER_FLUSHALL, RespCommand.CLUSTER_RESERVE])
                classes[(int)cmd] = OffloadClass.Always;

            foreach (var cmd in (ReadOnlySpan<RespCommand>)[
                RespCommand.SELECT, RespCommand.INFO, RespCommand.DEBUG, RespCommand.CLUSTER_APPENDLOG,
                RespCommand.CLUSTER_ADDSLOTS, RespCommand.CLUSTER_ADDSLOTSRANGE, RespCommand.CLUSTER_DELSLOTS,
                RespCommand.CLUSTER_DELSLOTSRANGE, RespCommand.CLUSTER_SETSLOT, RespCommand.CLUSTER_SETSLOTSRANGE,
                RespCommand.CLUSTER_MEET, RespCommand.CLUSTER_FORGET, RespCommand.CLUSTER_RESET,
                RespCommand.CLUSTER_SETCONFIGEPOCH, RespCommand.CLUSTER_BUMPEPOCH, RespCommand.CLUSTER_GOSSIP,
                RespCommand.CLUSTER_REPLICATE, RespCommand.CLUSTER_FAILOVER, RespCommand.CLUSTER_FAILSTOPWRITES,
                RespCommand.FAILOVER, RespCommand.REPLICAOF, RespCommand.SECONDARYOF])
                classes[(int)cmd] = OffloadClass.Conditional;

            return classes;
        }

        /// <summary>
        /// Whether <paramref name="cmd"/>, which has just been parsed, has to run on an executor thread: it would wait on
        /// the thread it runs on, which receives for other sessions, and this session can park to move it.
        /// </summary>
        /// <remarks>
        /// A command that does not wait is classed <see cref="OffloadClass.Never"/>, so it pays one load, and a few more
        /// if it may add records while a log has a storage tier or there is an AOF. A command already on an executor thread
        /// runs there, as does one in a consume that cannot park, such as a script's, and one resumed on the thread pool by
        /// an executor that has been disposed.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool MustOffload(RespCommand cmd)
        {
            var classes = OffloadClasses;
            if ((uint)cmd >= (uint)classes.Length)
                return false;

            var c = classes[(int)cmd];
            return ((c != OffloadClass.Never
                    && (c == OffloadClass.Always
                        || (c == OffloadClass.WhenSpilled ? storageSession.MayReadFromDisk : MustOffloadConditional(cmd))))
                    || (MayAddRecords(cmd) && storageSession.MayWaitToAllocate))
                && CanSuspend && !storeWrapper.offloadExecutor.RunsInPlace;
        }

        /// <summary>
        /// Whether <paramref name="cmd"/> may add records to the store's log or to the AOF, and so may wait for a flush
        /// before it can allocate them.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool MayAddRecords(RespCommand cmd)
            => MayAppendToAof(cmd)
                || cmd is RespCommand.CLUSTER_APPENDLOG or RespCommand.CLUSTER_MIGRATE
                    or RespCommand.CLUSTER_DELKEYSINSLOT or RespCommand.CLUSTER_DELKEYSINSLOTRANGE;

        /// <summary>
        /// Whether an EXEC, which has just been parsed against a transaction that has queued its commands, has to run on
        /// an executor thread: the queued commands may read from the device, or wait for a flush to add their records.
        /// Moving the EXEC before it locks anything runs the whole transaction there.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool MustOffloadExec()
            => (storageSession.MayReadFromDisk || storageSession.MayWaitToAllocate)
                && CanSuspend && !storeWrapper.offloadExecutor.RunsInPlace;

        /// <summary>
        /// Whether a command of class <see cref="OffloadClass.Conditional"/> has to run on an executor thread, decided
        /// from its arguments or from the server's configuration.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        bool MustOffloadConditional(RespCommand cmd)
        {
            switch (cmd)
            {
                case RespCommand.SELECT:
                    // Selecting a database for the first time creates it, with its log and its device
                    return parseState.Count == 1 && parseState.TryGetInt(0, out var index) && index != activeDbId
                        && index > 0 && index < storeWrapper.serverOptions.MaxDatabases
                        && !storeWrapper.TryGetDatabase(index, out _);

                case RespCommand.INFO:
                    // Its keyspace section scans the log of every database, the part on the device included
                    return storeWrapper.serverOptions.EnableStorageTier;

                case RespCommand.DEBUG:
                    // Its blocking subcommands park the session themselves; this one waits for the log's flush
                    return parseState.Count > 0 && parseState.GetArgSliceByRef(0).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FLUSHANDEVICT);

                case RespCommand.CLUSTER_APPENDLOG:
                    // Synchronous replay applies the stream to the store on this thread, where a record waits for the
                    // Vector Set replays queued before it, which other sublogs' streams may queue at any time. A replica
                    // that truncates its AOF fast resets it, on the device, when the stream skips ahead.
                    return (storeWrapper.runtimeConfig.GetInt(ServerConfigType.AOF_REPLAY_MAX_LAG_BYTES) == 0
                            && (storageSession.MayReadFromDisk || storeWrapper.serverOptions.EnableVectorSetPreview))
                        || (storeWrapper.serverOptions.FastAofTruncate && AppendLogSkipsAhead());

                default:
                    // The rest change the cluster configuration, which is written to its file as it changes unless
                    // flushes are batched
                    return storeWrapper.serverOptions.ClusterConfigFlushFrequencyMs == 0;
            }
        }

        /// <summary>
        /// Whether the CLUSTER APPENDLOG just parsed ships a chunk from an address the primary skipped ahead to, so that a
        /// replica that truncates its AOF fast resets the sublog before it enqueues the chunk.
        /// </summary>
        bool AppendLogSkipsAhead()
            => parseState.Count == 6
                && parseState.TryGetLong(2, allowLeadingZeros: true, out var previousAddress)
                && parseState.TryGetLong(3, allowLeadingZeros: true, out var currentAddress)
                && storeWrapper.appendOnlyFile is { } appendOnlyFile
                && GarnetAppendOnlyFile.ReplicaStreamSkipsAhead(previousAddress, currentAddress,
                    parseState.GetArgSliceByRef(5).Length, appendOnlyFile.Log.UnsafeGetLogPageSizeBits());

        /// <summary>
        /// Parks the session and has an executor thread resume it, so the command the read cursor has been rewound to is
        /// parsed, authorized and run again there.
        /// </summary>
        /// <param name="mustRun">
        /// Whether to start an executor thread for the session even when every one is busy, because the session holds
        /// locks that what the busy threads run may be waiting for.
        /// </param>
        /// <remarks>
        /// The park does not retain the batch scope, so the cluster epoch is released while the session waits for an
        /// executor thread.
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void ParkForOffload(bool mustRun)
        {
            ValueTask body;
            using (BeginAsyncCommand())
                body = OffloadAsync(mustRun);

            _ = CompleteAsyncCommand(body);
            Debug.Assert(asyncSuspended, "An offload always suspends");
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask OffloadAsync(bool mustRun)
            => await new OffloadAwaitable(this, mustRun);

        /// <summary>
        /// Parks a session that has suspended, and posts it to the executor if it suspended to be moved there.
        /// </summary>
        /// <returns>True if the caller must drive the resume, false if another thread will.</returns>
        /// <remarks>
        /// The post waits for the park: the executor resumes the session by claiming the parked suspension, which it
        /// can only do once the thread that suspended it has left the session.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool ParkSuspension()
        {
            var offload = deferredOffload;
            if (offload == 0)
                return handoff.Park();

            deferredOffload = 0;
            var box = resumeBox;

            // Nothing else holds the resume of an offload, so it cannot have arrived while the session was arming
            if (handoff.Park())
                Debug.Fail("An offload was resumed before it was posted");

            storeWrapper.offloadExecutor.Post(box, mustRun: offload == 2);
            return false;
        }

        /// <summary>
        /// Awaiter that suspends a command without waiting on anything, and has <see cref="ParkSuspension"/> post the
        /// session to the executor once it has parked.
        /// </summary>
        readonly struct OffloadAwaitable(RespServerSession session, bool mustRun) : ICriticalNotifyCompletion
        {
            public OffloadAwaitable GetAwaiter() => this;

            public bool IsCompleted => false;

            public void GetResult() { }

            // The continuation is the resume of the session's own suspension box, which the executor runs instead
            public void OnCompleted(Action continuation) => session.deferredOffload = mustRun ? (byte)2 : (byte)1;

            public void UnsafeOnCompleted(Action continuation) => session.deferredOffload = mustRun ? (byte)2 : (byte)1;
        }
    }
}