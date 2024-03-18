// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        // These are classes so instantiate once and re-initialize
        private ArrayKeyIterationFunctions.GetDBSize<SpanByte, SpanByte> mainStoreDbSizeFuncs;
        private ArrayKeyIterationFunctions.GetDBSize<byte[], IGarnetObject> objectStoreDbSizeFuncs;

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
        internal unsafe bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> keys, long count = 10, Span<byte> typeObject = default)
        {
            const long IsObjectStoreCursor = 1 << 49;
            Keys ??= new();
            Keys.Clear();

            objStoreKeys ??= new();
            objStoreKeys.Clear();

            keys = Keys;

            Type matchType = null;
            if (typeObject != default)
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

            fixed (byte* patternPtr = patternB.Bytes)
            {
                mainStoreDbScanFuncs ??= new();
                mainStoreDbScanFuncs.Initialize(Keys, allKeys ? null : patternPtr, patternB.Bytes.Length);
                objStoreDbScanFuncs ??= new();
                objStoreDbScanFuncs.Initialize(objStoreKeys, allKeys ? null : patternPtr, patternB.Bytes.Length, matchType);

                storeCursor = cursor;
                long remainingCount = count;

                // Cursor is zero or not an object store address
                // Scan main store only for string or default key type
                if ((cursor & IsObjectStoreCursor) == 0 && (typeObject == default || typeObject.SequenceEqual(CmdStrings.STRING) || typeObject.SequenceEqual(CmdStrings.stringt)))
                {
                    session.ScanCursor(ref storeCursor, count, mainStoreDbScanFuncs, validateCursor: cursor != 0 && cursor != lastScanCursor);
                    remainingCount -= Keys.Count;
                }

                // Scan object store with the type parameter
                // Check the cursor value corresponds to the object store
                if (objectStoreSession != null && remainingCount > 0 && (typeObject == default || (!typeObject.SequenceEqual(CmdStrings.STRING) && !typeObject.SequenceEqual(CmdStrings.stringt))))
                {
                    var validateCursor = storeCursor != 0 && storeCursor != lastScanCursor;
                    storeCursor &= ~IsObjectStoreCursor;
                    objectStoreSession.ScanCursor(ref storeCursor, remainingCount, objStoreDbScanFuncs, validateCursor: validateCursor);
                    if (storeCursor != 0) storeCursor = storeCursor | IsObjectStoreCursor;
                    Keys.AddRange(objStoreKeys);
                }
            }

            lastScanCursor = storeCursor;
            return true;
        }

        /// <summary>
        /// Iterate the contents of the main store
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <returns></returns>
        internal bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => session.Iterate(ref scanFunctions, untilAddress);

        /// <summary>
        /// Iterate the contents of the main store (pull based)
        /// </summary>
        internal ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => session.Iterate();

        /// <summary>
        /// Iterate the contents of the object store
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <returns></returns>
        internal bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => objectStoreSession.Iterate(ref scanFunctions, untilAddress);

        /// <summary>
        /// Iterate the contents of the main store (pull based)
        /// </summary>
        internal ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => objectStoreSession.Iterate();

        /// <summary>
        ///  Get a list of the keys in the store and object store
        ///  when using pattern
        /// </summary>
        /// <returns></returns>
        internal unsafe List<byte[]> DBKeys(ArgSlice pattern)
        {
            Keys ??= new();
            Keys.Clear();

            var allKeys = *pattern.ptr == '*' && pattern.Bytes.Length == 1;

            mainStoreDbKeysFuncs ??= new();
            mainStoreDbKeysFuncs.Initialize(Keys, allKeys ? null : pattern.ptr, pattern.Bytes.Length);
            session.Iterate(ref mainStoreDbKeysFuncs);

            if (objectStoreSession != null)
            {
                objStoreDbKeysFuncs ??= new();
                objStoreDbKeysFuncs.Initialize(Keys, allKeys ? null : pattern.ptr, pattern.Bytes.Length, matchType: null);
                objectStoreSession.Iterate(ref objStoreDbKeysFuncs);
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
            session.ScanCursor(ref cursor, long.MaxValue, mainStoreDbSizeFuncs);
            int count = mainStoreDbSizeFuncs.count;
            if (objectStoreSession != null)
            {
                objectStoreDbSizeFuncs ??= new();
                objectStoreDbSizeFuncs.Initialize();
                cursor = 0;
                objectStoreSession.ScanCursor(ref cursor, long.MaxValue, objectStoreDbSizeFuncs);
                count += objectStoreDbSizeFuncs.count;
            }

            return count;
        }

        internal static unsafe class ArrayKeyIterationFunctions
        {
            internal sealed class MainStoreGetDBKeys : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                List<byte[]> keys;
                byte* patternB;
                int patternLength;

                internal void Initialize(List<byte[]> keys, byte* patternB, int length)
                {
                    this.keys = keys;
                    this.patternB = patternB;
                    this.patternLength = length;
                }

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                        => ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    if (patternB != null && !GlobUtils.Match(patternB, patternLength, key.ToPointer(), key.Length, true))
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                    }
                    else
                    {
                        cursorRecordResult = CursorRecordResult.Accept;
                        keys.Add(key.ToByteArray());
                    }
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreGetDBKeys : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                List<byte[]> keys;
                byte* patternB;
                int patternLength;
                private Type matchType;

                internal void Initialize(List<byte[]> keys, byte* patternB, int length, Type matchType = null)
                {
                    this.keys = keys;
                    this.patternB = patternB;
                    this.patternLength = length;
                    this.matchType = matchType;
                }

                public bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool ConcurrentReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    if (patternB != null)
                    {
                        fixed (byte* keyPtr = key)
                        {
                            if (!GlobUtils.Match(patternB, patternLength, keyPtr, key.Length, true))
                            {
                                cursorRecordResult = CursorRecordResult.Skip;
                                return true;
                            }
                        }
                    }

                    if (matchType != null && value.GetType() != matchType)
                    {
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    keys.Add(key);
                    cursorRecordResult = CursorRecordResult.Accept;
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class GetDBSize<TKey, TValue> : IScanIteratorFunctions<TKey, TValue>
            {
                // This must be a class as it is passed through pending IO operations
                internal int count;

                internal void Initialize() => count = 0;

                public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    ++count;
                    return true;
                }
                public bool ConcurrentReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }
        }
    }
}