// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        internal static class ClusterKeyIterationFunctions
        {
            internal class KeyIterationInfo
            {
                // This must be a class as it is passed through pending IO operations, so it is wrapped by higher structures for inlining as a generic type arg.
                internal int keyCount;
                internal readonly int slot;

                internal KeyIterationInfo(int slot) => this.slot = slot;
            }

            internal sealed class MainStoreCountKeys : IScanIteratorFunctions
            {
                private readonly KeyIterationInfo info;
                // This must be a class as it is passed through pending IO operations

                internal int KeyCount { get => info.keyCount; set => info.keyCount = value; }
                internal int Slot => info.slot;

                internal MainStoreCountKeys(int slot) => info = new(slot);

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    if (HashSlotUtils.HashSlot(srcLogRecord.Key) == Slot && !Expired(ref srcLogRecord))
                        KeyCount++;
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreCountKeys : IScanIteratorFunctions
            {
                private readonly KeyIterationInfo info;
                // This must be a class as it is passed through pending IO operations

                internal int KeyCount { get => info.keyCount; set => info.keyCount = value; }
                internal int Slot => info.slot;

                internal ObjectStoreCountKeys(int slot) => info = new(slot);

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here , out CursorRecordResult cursorRecordResult
                    if (HashSlotUtils.HashSlot(srcLogRecord.Key) == Slot && !Expired(ref srcLogRecord))
                        KeyCount++;
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal readonly struct MainStoreGetKeysInSlot : IScanIteratorFunctions
            {
                readonly List<byte[]> keys;
                readonly int slot, maxKeyCount;

                internal MainStoreGetKeysInSlot(List<byte[]> keys, int slot, int maxKeyCount)
                {
                    this.keys = keys;
                    this.slot = slot;
                    this.maxKeyCount = maxKeyCount;
                }

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here, out CursorRecordResult cursorRecordResult
                    var key = srcLogRecord.Key;
                    if (HashSlotUtils.HashSlot(key) == slot && !Expired(ref srcLogRecord))
                        keys.Add(key.ToArray());
                    return keys.Count < maxKeyCount;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal readonly struct ObjectStoreGetKeysInSlot : IScanIteratorFunctions
            {
                readonly List<byte[]> keys;
                readonly int slot;

                internal ObjectStoreGetKeysInSlot(List<byte[]> keys, int slot)
                {
                    this.keys = keys;
                    this.slot = slot;
                }

                public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    var key = srcLogRecord.Key;
                    if (HashSlotUtils.HashSlot(key) == slot && !Expired<TSourceLogRecord>(ref srcLogRecord))
                        keys.Add(key.ToArray());
                    return true;
                }

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }
        }
    }
}