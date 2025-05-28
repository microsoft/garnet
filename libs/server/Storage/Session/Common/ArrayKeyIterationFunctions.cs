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
        internal unsafe bool DbScan(PinnedSpanByte patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> keys, long count = 10, ReadOnlySpan<byte> typeObject = default)
        {
            const long IsObjectStoreCursor = 1L << LogAddress.kAddressBits;
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

            byte* patternPtr = patternB.ToPointer();

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
        /// Iterate the contents of the main store (push-based)
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <returns></returns>
        internal bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions
            => basicContext.Session.IterateLookup(ref scanFunctions, untilAddress);

        /// <summary>
        /// Iterate the contents of the main store (pull based)
        /// </summary>
        internal ITsavoriteScanIterator IterateMainStore()
            => basicContext.Session.Iterate();

        /// <summary>
        /// Iterate the contents of the object store (push-based)
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <returns></returns>
        internal bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions
            => objectStoreBasicContext.Session.IterateLookup(ref scanFunctions, untilAddress);

        /// <summary>
        /// Iterate the contents of the main store (pull based)
        /// </summary>
        internal ITsavoriteScanIterator IterateObjectStore()
            => objectStoreBasicContext.Session.Iterate();

        /// <summary>
        ///  Get a list of the keys in the store and object store when using pattern
        /// </summary>
        /// <returns></returns>
        internal unsafe List<byte[]> DBKeys(PinnedSpanByte pattern)
        {
            Keys ??= new();
            Keys.Clear();

            var allKeys = *pattern.ToPointer() == '*' && pattern.Length == 1;

            mainStoreDbKeysFuncs ??= new();
            mainStoreDbKeysFuncs.Initialize(Keys, allKeys ? null : pattern.ToPointer(), pattern.Length);
            basicContext.Session.Iterate(ref mainStoreDbKeysFuncs);

            if (!objectStoreBasicContext.IsNull)
            {
                objStoreDbKeysFuncs ??= new();
                objStoreDbKeysFuncs.Initialize(Keys, allKeys ? null : pattern.ToPointer(), pattern.Length, matchType: null);
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
                objectStoreBasicContext.Session.ScanCursor(ref cursor, long.MaxValue, objectStoreDbSizeFuncs);
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

            internal sealed class MainStoreGetDBKeys : IScanIteratorFunctions
            {
                private readonly GetDBKeysInfo info;

                internal MainStoreGetDBKeys() => info = new();

                internal void Initialize(List<byte[]> keys, byte* patternB, int length)
                    => info.Initialize(keys, patternB, length);

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    var key = logRecord.Key;

                    if (MainSessionFunctions.CheckExpiry(in logRecord))
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    if (info.patternB != null)
                    {
                        bool ok;
                        if (logRecord.IsPinnedKey)
                            ok = GlobUtils.Match(info.patternB, info.patternLength, logRecord.PinnedKeyPointer, key.Length, true);
                        else
                            fixed(byte* keyPtr = key)
                                ok = GlobUtils.Match(info.patternB, info.patternLength, keyPtr, key.Length, true);
                        if (!ok)
                        {
                            cursorRecordResult = CursorRecordResult.Skip;
                            return true;
                        }
                    }

                    info.keys.Add(key.ToArray());
                    cursorRecordResult = CursorRecordResult.Accept;
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreGetDBKeys : IScanIteratorFunctions
            {
                private readonly GetDBKeysInfo info;

                internal ObjectStoreGetDBKeys() => info = new();

                internal void Initialize(List<byte[]> keys, byte* patternB, int length, Type matchType = null)
                    => info.Initialize(keys, patternB, length, matchType);

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    if (ObjectSessionFunctions.CheckExpiry(ref logRecord))
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

                    if (info.matchType != null && logRecord.ValueObject.GetType() != info.matchType)
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

            internal sealed class MainStoreGetDBSize : IScanIteratorFunctions
            {
                private readonly GetDBSizeInfo info;

                internal int Count => info.count;

                internal MainStoreGetDBSize() => info = new();

                internal void Initialize() => info.Initialize();

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Skip;
                    if (!MainSessionFunctions.CheckExpiry(in logRecord))
                    {
                        ++info.count;
                    }
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreGetDBSize : IScanIteratorFunctions
            {
                private readonly GetDBSizeInfo info;

                internal int Count => info.count;

                internal ObjectStoreGetDBSize() => info = new();

                internal void Initialize() => info.Initialize();

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Skip;
                    if (!ObjectSessionFunctions.CheckExpiry(ref logRecord))
                    {
                        ++info.count;
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