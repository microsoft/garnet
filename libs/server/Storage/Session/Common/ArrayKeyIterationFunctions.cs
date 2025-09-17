// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        // These contain classes so instantiate once and re-initialize
        private ArrayKeyIterationFunctions.MainStoreGetDBSize mainStoreDbSizeFuncs;
        private ArrayKeyIterationFunctions.ObjectStoreGetDBSize objectStoreDbSizeFuncs;

        // Iterators for SCAN command
        private ArrayKeyIterationFunctions.MainStoreGetDBKeys mainStoreDbScanFuncs;
        private ArrayKeyIterationFunctions.ObjectStoreGetDBKeys objStoreDbScanFuncs;

        // Iterators for expired key deletion
        private ArrayKeyIterationFunctions.MainStoreExpiredKeyDeletionScan mainStoreExpiredKeyDeletionScanFuncs;
        private ArrayKeyIterationFunctions.ObjectStoreExpiredKeyDeletionScan objectStoreExpiredKeyDeletionScanFuncs;

        // Iterators for KEYS command
        private ArrayKeyIterationFunctions.MainStoreGetDBKeys mainStoreDbKeysFuncs;
        private ArrayKeyIterationFunctions.ObjectStoreGetDBKeys objStoreDbKeysFuncs;

        long lastScanCursor;
        List<byte[]> objStoreKeys;
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
        internal unsafe bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> keys, long count = 10, ReadOnlySpan<byte> typeObject = default)
        {
            const long IsObjectStoreCursor = 1L << 49;
            Keys ??= new();
            Keys.Clear();

            objStoreKeys ??= new();
            objStoreKeys.Clear();

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
                else if (!typeObject.SequenceEqual(CmdStrings.STRING) && !typeObject.SequenceEqual(CmdStrings.stringt))
                {
                    // Unexpected typeObject type
                    storeCursor = lastScanCursor = 0;
                    return true;
                }
            }

            byte* patternPtr = patternB.ptr;

            mainStoreDbScanFuncs ??= new();
            mainStoreDbScanFuncs.Initialize(Keys, allKeys ? null : patternPtr, patternB.Length);
            objStoreDbScanFuncs ??= new();
            objStoreDbScanFuncs.Initialize(objStoreKeys, allKeys ? null : patternPtr, patternB.Length, matchType);

            storeCursor = cursor;
            long remainingCount = count;

            // Cursor is zero or not an object store address
            // Scan main store only for string or default key type
            if ((cursor & IsObjectStoreCursor) == 0 && (typeObject.IsEmpty || typeObject.SequenceEqual(CmdStrings.STRING) || typeObject.SequenceEqual(CmdStrings.stringt)))
            {
                basicContext.Session.ScanCursor(ref storeCursor, count, mainStoreDbScanFuncs, validateCursor: cursor != 0 && cursor != lastScanCursor);
                remainingCount -= Keys.Count;
            }

            // Scan object store with the type parameter
            // Check the cursor value corresponds to the object store
            if (!objectStoreBasicContext.IsNull && remainingCount > 0 && (typeObject.IsEmpty || (!typeObject.SequenceEqual(CmdStrings.STRING) && !typeObject.SequenceEqual(CmdStrings.stringt))))
            {
                var validateCursor = storeCursor != 0 && storeCursor != lastScanCursor;
                storeCursor &= ~IsObjectStoreCursor;
                objectStoreBasicContext.Session.ScanCursor(ref storeCursor, remainingCount, objStoreDbScanFuncs, validateCursor: validateCursor);
                if (storeCursor != 0)
                    storeCursor |= IsObjectStoreCursor;
                Keys.AddRange(objStoreKeys);
            }

            lastScanCursor = storeCursor;
            return true;
        }

        /// <summary>
        /// Iterates over main store memory collecting expired records.
        /// </summary>
        internal (long, long) MainStoreExpiredKeyDeletionScan(long fromAddress, long untilAddress)
        {
            mainStoreExpiredKeyDeletionScanFuncs ??= new();
            mainStoreExpiredKeyDeletionScanFuncs.Initialize(this);
            _ = basicContext.Session.ScanCursor(ref fromAddress, untilAddress, mainStoreExpiredKeyDeletionScanFuncs);
            return (mainStoreExpiredKeyDeletionScanFuncs.deletedCount, mainStoreExpiredKeyDeletionScanFuncs.totalCount);
        }

        /// <summary>
        /// Iterates over object store memory collecting expired records.
        /// </summary>
        internal (long, long) ObjectStoreExpiredKeyDeletionScan(long fromAddress, long untilAddress)
        {
            objectStoreExpiredKeyDeletionScanFuncs ??= new();
            objectStoreExpiredKeyDeletionScanFuncs.Initialize(this);
            _ = objectStoreBasicContext.Session.ScanCursor(ref fromAddress, untilAddress, objectStoreExpiredKeyDeletionScanFuncs);
            return (objectStoreExpiredKeyDeletionScanFuncs.deletedCount, objectStoreExpiredKeyDeletionScanFuncs.totalCount);
        }

        /// <summary>
        /// Iterate the contents of the main store (push-based)
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <param name="cursor"></param>
        /// <param name="maxAddress"></param>
        /// <param name="validateCursor"></param>
        /// <param name="includeTombstones"></param>
        /// <returns></returns>
        internal bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool validateCursor = false, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => basicContext.Session.IterateLookup(ref scanFunctions, ref cursor, untilAddress, validateCursor: validateCursor, maxAddress: maxAddress, resetCursor: false, includeTombstones: includeTombstones);

        /// <summary>
        /// Iterate the contents of the main store (pull based)
        /// </summary>
        internal ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => basicContext.Session.Iterate();

        /// <summary>
        /// Iterate the contents of the object store (push-based)
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <param name="cursor"></param>
        /// <param name="maxAddress"></param>
        /// <param name="validateCursor"></param>
        /// <param name="includeTombstones"></param>
        /// <returns></returns>
        internal bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool validateCursor = false, bool includeTombstones = false)
           where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => objectStoreBasicContext.Session.IterateLookup(ref scanFunctions, ref cursor, untilAddress, validateCursor: validateCursor, maxAddress: maxAddress, resetCursor: false, includeTombstones: includeTombstones);

        /// <summary>
        /// Iterate the contents of the main store (pull based)
        /// </summary>
        internal ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => objectStoreBasicContext.Session.Iterate();

        /// <summary>
        ///  Get a list of the keys in the store and object store
        ///  when using pattern
        /// </summary>
        /// <returns></returns>
        internal unsafe List<byte[]> DBKeys(ArgSlice pattern)
        {
            Keys ??= new();
            Keys.Clear();

            var allKeys = *pattern.ptr == '*' && pattern.Length == 1;

            mainStoreDbKeysFuncs ??= new();
            mainStoreDbKeysFuncs.Initialize(Keys, allKeys ? null : pattern.ptr, pattern.Length);
            basicContext.Session.Iterate(ref mainStoreDbKeysFuncs);

            if (!objectStoreBasicContext.IsNull)
            {
                objStoreDbKeysFuncs ??= new();
                objStoreDbKeysFuncs.Initialize(Keys, allKeys ? null : pattern.ptr, pattern.Length, matchType: null);
                objectStoreBasicContext.Session.Iterate(ref objStoreDbKeysFuncs);
            }

            return Keys;
        }

        /// <summary>
        /// Count the number of keys in main and object store
        /// </summary>
        /// <returns></returns>
        internal int DbSize()
        {
            mainStoreDbSizeFuncs ??= new();
            mainStoreDbSizeFuncs.Initialize();
            long cursor = 0;
            basicContext.Session.ScanCursor(ref cursor, long.MaxValue, mainStoreDbSizeFuncs);
            int count = mainStoreDbSizeFuncs.Count;
            if (objectStoreBasicContext.Session != null)
            {
                objectStoreDbSizeFuncs ??= new();
                objectStoreDbSizeFuncs.Initialize();
                cursor = 0;
                _ = objectStoreBasicContext.Session.ScanCursor(ref cursor, long.MaxValue, objectStoreDbSizeFuncs);
                count += objectStoreDbSizeFuncs.Count;
            }

            return count;
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

            internal sealed class ObjectStoreExpiredKeyDeletionScan : ExpiredKeysBase<byte[], IGarnetObject>
            {
                protected override bool IsExpired(ref IGarnetObject value) => value.Expiration > 0 && ObjectSessionFunctions.CheckExpiry(value);
                protected override bool DeleteIfExpiredInMemory(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata)
                {
                    var input = new ObjectInput(new RespInputHeader(GarnetObjectType.DelIfExpIm));
                    var output = new GarnetObjectStoreOutput();
                    return GarnetStatus.OK == storageSession.RMW_ObjectStore(ref key, ref input, ref output, ref storageSession.objectStoreBasicContext);
                }
            }

            internal sealed class MainStoreExpiredKeyDeletionScan : ExpiredKeysBase<SpanByte, SpanByte>
            {
                protected override bool IsExpired(ref SpanByte value) => value.MetadataSize > 0 && MainSessionFunctions.CheckExpiry(ref value);
                protected override bool DeleteIfExpiredInMemory(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata)
                {
                    var input = new RawStringInput(RespCommand.DELIFEXPIM);
                    return GarnetStatus.OK == storageSession.DEL_Conditional(ref key, ref input, ref storageSession.basicContext);
                }
            }

            internal abstract class ExpiredKeysBase<TKey, TValue> : IScanIteratorFunctions<TKey, TValue>
            {
                public long totalCount;
                public long deletedCount;
                protected StorageSession storageSession;

                public void Initialize(StorageSession storageSession)
                    => this.storageSession = storageSession;

                protected abstract bool IsExpired(ref TValue value);

                protected abstract bool DeleteIfExpiredInMemory(ref TKey key, ref TValue value, RecordMetadata recordMetadata);

                public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                        => ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    totalCount++;
                    if (IsExpired(ref value))
                    {
                        cursorRecordResult = CursorRecordResult.Accept;
                        if (DeleteIfExpiredInMemory(ref key, ref value, recordMetadata))
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

            internal sealed class MainStoreGetDBKeys : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                private readonly GetDBKeysInfo info;

                internal MainStoreGetDBKeys() => info = new();

                internal void Initialize(List<byte[]> keys, byte* patternB, int length)
                    => info.Initialize(keys, patternB, length);

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                        => ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    if ((info.patternB != null && !GlobUtils.Match(info.patternB, info.patternLength, key.ToPointer(), key.Length, true))
                        || (value.MetadataSize != 0 && MainSessionFunctions.CheckExpiry(ref value)))
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                    }
                    else
                    {
                        cursorRecordResult = CursorRecordResult.Accept;
                        info.keys.Add(key.ToByteArray());
                    }
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreGetDBKeys : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                private readonly GetDBKeysInfo info;

                internal ObjectStoreGetDBKeys() => info = new();

                internal void Initialize(List<byte[]> keys, byte* patternB, int length, Type matchType = null)
                    => info.Initialize(keys, patternB, length, matchType);

                public bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool ConcurrentReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    if (value.Expiration > 0 && ObjectSessionFunctions.CheckExpiry(value))
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    if (info.patternB != null)
                    {
                        fixed (byte* keyPtr = key)
                        {
                            if (!GlobUtils.Match(info.patternB, info.patternLength, keyPtr, key.Length, true))
                            {
                                cursorRecordResult = CursorRecordResult.Skip;
                                return true;
                            }
                        }
                    }

                    if (info.matchType != null && value.GetType() != info.matchType)
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    info.keys.Add(key);
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

            internal sealed class MainStoreGetDBSize : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                private readonly GetDBSizeInfo info;

                internal int Count => info.count;

                internal MainStoreGetDBSize() => info = new();

                internal void Initialize() => info.Initialize();

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Skip;
                    if (value.MetadataSize == 0 || !MainSessionFunctions.CheckExpiry(ref value))
                    {
                        ++info.count;
                    }
                    return true;
                }
                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreGetDBSize : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                private readonly GetDBSizeInfo info;

                internal int Count => info.count;

                internal ObjectStoreGetDBSize() => info = new();

                internal void Initialize() => info.Initialize();

                public bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Skip;
                    if (value.Expiration == 0 || !ObjectSessionFunctions.CheckExpiry(value))
                    {
                        ++info.count;
                    }
                    return true;
                }
                public bool ConcurrentReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }
        }
    }
}