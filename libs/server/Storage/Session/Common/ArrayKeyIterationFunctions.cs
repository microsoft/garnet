// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using static Garnet.server.StorageSession.ArrayKeyIterationFunctions;
#pragma warning disable IDE0005 // Using directive is unnecessary.
    using static LogRecordUtils;

    sealed partial class StorageSession : IDisposable
    {
        // These contain classes so instantiate once and re-initialize
        private ArrayKeyIterationFunctions.UnifiedStoreGetDBSize unifiedStoreDbSizeFuncs;

        // Iterator for SCAN command
        private ArrayKeyIterationFunctions.UnifiedStoreGetDBKeys unifiedStoreDbScanFuncs;

        // Iterator for expired key deletion
        private ArrayKeyIterationFunctions.ExpiredKeyDeletionScan expiredKeyDeletionScanFuncs;

        // Iterator for KEYS command
        private ArrayKeyIterationFunctions.UnifiedStoreGetDBKeys unifiedStoreDbKeysFuncs;

        // Iterator for cluster slot deletion (DeleteSlotKeys)
        private ArrayKeyIterationFunctions.DeleteSlotKeysScan deleteSlotKeysFuncs;

        long lastScanCursor;
        List<byte[]> Keys;

        /// <summary>
        ///  Gets keys matching the pattern with a limit of count in every iteration
        ///  when using pattern
        /// </summary>
        /// <param name="patternB">ptr to the matching pattern</param>
        /// <param name="allKeys">true when the pattern is *</param>
        /// <param name="cursor">cursor sent in the request</param>
        /// <param name="storeCursor"></param>
        /// <param name="keys">The list with the keys from the store</param>
        /// <param name="count">size of every block or keys to return</param>
        /// <param name="typeObject">The type object to filter out</param>
        /// <returns></returns>
        internal unsafe bool DbScan(PinnedSpanByte patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> keys, long count = 10, ReadOnlySpan<byte> typeObject = default)
        {
            Keys ??= new();
            Keys.Clear();

            keys = Keys;

            Type matchType = null;
            if (!typeObject.IsEmpty)
            {
                if (typeObject.SequenceEqual(CmdStrings.ZSET) || typeObject.SequenceEqual(CmdStrings.zset))
                {
                    matchType = typeof(SortedSetObject);
                }
                else if (typeObject.SequenceEqual(CmdStrings.LIST) || typeObject.SequenceEqual(CmdStrings.list))
                {
                    matchType = typeof(ListObject);
                }
                else if (typeObject.SequenceEqual(CmdStrings.SET) || typeObject.SequenceEqual(CmdStrings.set))
                {
                    matchType = typeof(SetObject);
                }
                else if (typeObject.SequenceEqual(CmdStrings.HASH) || typeObject.SequenceEqual(CmdStrings.hash))
                {
                    matchType = typeof(HashObject);
                }
                else if (typeObject.SequenceEqual(CmdStrings.STRING) || typeObject.SequenceEqual(CmdStrings.stringt))
                {
                    matchType = typeof(string);
                }
                else if (!typeObject.SequenceEqual(CmdStrings.STRING) && !typeObject.SequenceEqual(CmdStrings.stringt))
                {
                    // Unexpected typeObject type
                    storeCursor = lastScanCursor = 0;
                    return true;
                }
            }

            var patternPtr = patternB.ToPointer();

            unifiedStoreDbScanFuncs ??= IsConsistentReadSession ? new ConsistentUnifiedStoreGetDBKeys(readSessionState) : new UnifiedStoreGetDBKeys();
            unifiedStoreDbScanFuncs.Initialize(Keys, allKeys ? null : patternPtr, patternB.Length, matchType);

            storeCursor = cursor;
            long remainingCount = count;

            unifiedBasicContext.Session.ScanCursor(ref storeCursor, count, unifiedStoreDbScanFuncs, validateCursor: cursor != 0 && cursor != lastScanCursor);

            lastScanCursor = storeCursor;
            return true;
        }

        /// <summary>
        /// Iterates over store memory collecting expired records.
        /// </summary>
        internal (long, long) ExpiredKeyDeletionScan(long fromAddress, long untilAddress)
        {
            expiredKeyDeletionScanFuncs ??= new();
            expiredKeyDeletionScanFuncs.Initialize(this);
            _ = unifiedBasicContext.Session.ScanCursor(ref fromAddress, untilAddress, expiredKeyDeletionScanFuncs);
            return (expiredKeyDeletionScanFuncs.deletedCount, expiredKeyDeletionScanFuncs.totalCount);
        }

        /// <summary>
        /// Iterate the contents of the store (push-based)
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <param name="cursor"></param>
        /// <param name="maxAddress"></param>
        /// <param name="validateCursor"></param>
        /// <param name="includeTombstones"></param>
        /// <returns></returns>
        internal bool IterateStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool validateCursor = false, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions
            => stringBasicContext.Session.IterateLookup(ref scanFunctions, ref cursor, untilAddress, validateCursor: validateCursor, maxAddress: maxAddress, resetCursor: false, includeTombstones: includeTombstones);

        /// <summary>
        /// Delete every live key whose hash slot is in <paramref name="slots"/>.
        /// Uses lookup-based push iteration over the unified context (no <c>tempKv</c>) with
        /// snapshot semantics: every key live at scan-start whose slot matches is deleted.
        /// Preserves the previous pull-iterator semantics — every matched live key is deleted,
        /// including expired-but-not-yet-tombstoned records (no expiry filter).
        /// </summary>
        /// <param name="slots">Hash slot set to delete.</param>
        internal void DeleteSlotKeys(HashSet<int> slots)
        {
            deleteSlotKeysFuncs ??= new();
            deleteSlotKeysFuncs.Initialize(this, slots);

            // Snapshot semantics: ensures records RCU'd above TailAddress during the scan are
            // not silently suppressed (which would leave keys behind).
            _ = unifiedBasicContext.Session.IterateLookupSnapshot(ref deleteSlotKeysFuncs);
        }

        /// <summary>
        ///  Get a list of the keys in the store and object store when using pattern
        /// </summary>
        /// <returns></returns>
        internal unsafe List<byte[]> DBKeys(PinnedSpanByte pattern)
        {
            Keys ??= new();
            Keys.Clear();

            var allKeys = *pattern.ToPointer() == '*' && pattern.Length == 1;

            unifiedStoreDbKeysFuncs ??= IsConsistentReadSession ? new ConsistentUnifiedStoreGetDBKeys(readSessionState) : new UnifiedStoreGetDBKeys();
            unifiedStoreDbKeysFuncs.Initialize(Keys, allKeys ? null : pattern.ToPointer(), pattern.Length);

            // Snapshot semantics: emit each unique live key exactly once based on its latest in-range
            // version at scan-start, even if a concurrent RCU moves the key's tail above the captured
            // TailAddress during the scan. Equivalent to the legacy tempKv-backed Iterate(...).
            _ = unifiedBasicContext.Session.IterateLookupSnapshot(ref unifiedStoreDbKeysFuncs);

            return Keys;
        }

        /// <summary>
        /// Count the number of keys in main and object store
        /// </summary>
        /// <returns></returns>
        internal int DbSize()
        {
            unifiedStoreDbSizeFuncs ??= new();
            unifiedStoreDbSizeFuncs.Initialize();
            long cursor = 0;
            unifiedBasicContext.Session.ScanCursor(ref cursor, long.MaxValue, unifiedStoreDbSizeFuncs);

            return unifiedStoreDbSizeFuncs.Count;
        }

        internal static unsafe class ArrayKeyIterationFunctions
        {
            internal class GetDBKeysInfo
            {
                // This must be a class as it is passed through pending IO operations, so it is wrapped by higher structures for inlining as a generic type arg.
                internal List<byte[]> keys;
                internal byte* patternB;
                internal int patternLength;
                internal Type matchType;

                internal void Initialize(List<byte[]> keys, byte* patternB, int length, Type matchType = null)
                {
                    this.keys = keys;
                    this.patternB = patternB;
                    this.patternLength = length;
                    this.matchType = matchType;
                }
            }

            internal sealed class ExpiredKeyDeletionScan : ExpiredKeysBase
            {
                protected override bool DeleteIfExpiredInMemory<TSourceLogRecord>(in TSourceLogRecord logRecord,
                    RecordMetadata recordMetadata)
                    => GarnetStatus.OK == storageSession.DELIFEXPIM(PinnedSpanByte.FromPinnedSpan(logRecord.Key),
                        ref storageSession.unifiedBasicContext);
            }

            internal abstract class ExpiredKeysBase : IScanIteratorFunctions
            {
                public long totalCount;
                public long deletedCount;
                protected StorageSession storageSession;

                public void Initialize(StorageSession storageSession)
                    => this.storageSession = storageSession;

                protected abstract bool DeleteIfExpiredInMemory<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata)
                    where TSourceLogRecord : ISourceLogRecord;

                public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    totalCount++;
                    if (CheckExpiry(in logRecord))
                    {
                        cursorRecordResult = CursorRecordResult.Accept;
                        if (DeleteIfExpiredInMemory(in logRecord, recordMetadata))
                            deletedCount++;
                    }
                    else
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                    }

                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress)
                {
                    totalCount = deletedCount = 0;
                    return true;
                }

                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ConsistentUnifiedStoreGetDBKeys : UnifiedStoreGetDBKeys
            {
                readonly ReadSessionState readSessionState;
                internal ConsistentUnifiedStoreGetDBKeys(ReadSessionState readSessionState) : base()
                    => this.readSessionState = readSessionState;

                public override bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    readSessionState.BeforeConsistentReadKeyCallback(GarnetLog.HASH(logRecord.Key));
                    var status = base.Reader(in logRecord, recordMetadata, numberOfRecords, out cursorRecordResult);
                    readSessionState.AfterConsistentReadKeyCallback();
                    return status;
                }
            }

            internal class UnifiedStoreGetDBKeys : IScanIteratorFunctions
            {
                private readonly GetDBKeysInfo info;

                internal UnifiedStoreGetDBKeys() => info = new();

                internal void Initialize(List<byte[]> keys, byte* patternB, int length, Type matchType = null)
                    => info.Initialize(keys, patternB, length, matchType);

                public virtual bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    if (CheckExpiry(in logRecord))
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    var key = logRecord.Key;
                    if (info.patternB != null)
                    {
                        bool ok;
                        if (logRecord.IsPinnedKey)
                            ok = GlobUtils.Match(info.patternB, info.patternLength, logRecord.PinnedKeyPointer, key.Length, true);
                        else
                            fixed (byte* keyPtr = key)
                                ok = GlobUtils.Match(info.patternB, info.patternLength, keyPtr, key.Length, true);
                        if (!ok)
                        {
                            cursorRecordResult = CursorRecordResult.Skip;
                            return true;
                        }
                    }

                    if (info.matchType != null &&
                        ((logRecord.Info.ValueIsObject && (info.matchType == typeof(string) || info.matchType != logRecord.ValueObject.GetType())) ||
                         (!logRecord.Info.ValueIsObject && info.matchType != typeof(string))))
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    info.keys.Add(key.ToArray());
                    cursorRecordResult = CursorRecordResult.Accept;
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal class GetDBSizeInfo
            {
                // This must be a class as it is passed through pending IO operations, so it is wrapped by higher structures for inlining as a generic type arg.
                internal int count;

                internal void Initialize() => count = 0;
            }

            internal sealed class UnifiedStoreGetDBSize : IScanIteratorFunctions
            {
                private readonly GetDBSizeInfo info;

                internal int Count => info.count;

                internal UnifiedStoreGetDBSize() => info = new();

                internal void Initialize() => info.Initialize();

                public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Skip;
                    if (!CheckExpiry(in logRecord))
                        ++info.count;
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            /// <summary>
            /// Lookup-based push iterator callback that deletes every live key whose hash slot is
            /// in the supplied set. Cached on <see cref="StorageSession"/> via the
            /// <c>deleteSlotKeysFuncs</c> field; re-initialised per call.
            /// IMPORTANT: matches the previous pull-iterator semantics — every matched live key is
            /// deleted, including expired-but-not-yet-tombstoned records (no expiry filter).
            /// </summary>
            internal sealed class DeleteSlotKeysScan : IScanIteratorFunctions
            {
                private StorageSession storageSession;
                private HashSet<int> slots;

                internal void Initialize(StorageSession storageSession, HashSet<int> slots)
                {
                    this.storageSession = storageSession;
                    this.slots = slots;
                }

                public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Skip;
                    if (slots.Contains(HashSlotUtils.HashSlot(logRecord.Key)))
                    {
                        _ = storageSession.DELETE(PinnedSpanByte.FromPinnedSpan(logRecord.Key), ref storageSession.unifiedBasicContext);
                        cursorRecordResult = CursorRecordResult.Accept;
                    }
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            /// <summary>
            /// Lookup-based push iterator callback that returns true from <see cref="Found"/> if
            /// any live record's key hashes to a slot in the supplied set. Stops scanning on the
            /// first match by returning <c>false</c> from <see cref="Reader"/>. Cached on
            /// <see cref="StoreWrapper"/> via the <c>hasKeysInSlotsFuncs</c> field; re-initialised per call.
            /// </summary>
            internal sealed class HasKeysInSlotsScan : IScanIteratorFunctions
            {
                private List<int> slots;
                internal bool Found;

                internal void Initialize(List<int> slots)
                {
                    this.slots = slots;
                    Found = false;
                }

                public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Skip;
                    if (slots.Contains(HashSlotUtils.HashSlot(logRecord.Key)))
                    {
                        Found = true;
                        return false; // early exit
                    }
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }
        }
    }
}