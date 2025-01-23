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

            internal sealed class MainStoreCountKeys : IScanIteratorFunctions<SpanByte>
            {
                private readonly KeyIterationInfo info;
                // This must be a class as it is passed through pending IO operations

                internal int KeyCount { get => info.keyCount; set => info.keyCount = value; }
                internal int Slot => info.slot;

                internal MainStoreCountKeys(int slot) => info = new(slot);

                public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<SpanByte>
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    if (HashSlotUtils.HashSlot(srcLogRecord.Key) == Slot && !Expired<SpanByte, TSourceLogRecord>(ref srcLogRecord))
                        KeyCount++;
                    return true;
                }
                public bool ConcurrentReader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<SpanByte>
                    => SingleReader(ref logRecord, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreCountKeys : IScanIteratorFunctions<IGarnetObject>
            {
                private readonly KeyIterationInfo info;
                // This must be a class as it is passed through pending IO operations

                internal int KeyCount { get => info.keyCount; set => info.keyCount = value; }
                internal int Slot => info.slot;

                internal ObjectStoreCountKeys(int slot) => info = new(slot);

                public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here , out CursorRecordResult cursorRecordResult
                    if (HashSlotUtils.HashSlot(srcLogRecord.Key) == Slot && !Expired<IGarnetObject, TSourceLogRecord>(ref srcLogRecord))
                        KeyCount++;
                    return true;
                }
                public bool ConcurrentReader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
                    => SingleReader(ref logRecord, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal readonly struct MainStoreGetKeysInSlot : IScanIteratorFunctions<SpanByte>
            {
                readonly List<byte[]> keys;
                readonly int slot, maxKeyCount;

                internal MainStoreGetKeysInSlot(List<byte[]> keys, int slot, int maxKeyCount)
                {
                    this.keys = keys;
                    this.slot = slot;
                    this.maxKeyCount = maxKeyCount;
                }

                public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<SpanByte>
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here, out CursorRecordResult cursorRecordResult
                    var key = srcLogRecord.Key;
                    if (HashSlotUtils.HashSlot(key) == slot && !Expired<SpanByte, TSourceLogRecord>(ref srcLogRecord))
                        keys.Add(key.ToByteArray());
                    return keys.Count < maxKeyCount;
                }

                public bool ConcurrentReader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<SpanByte>
                    => SingleReader(ref logRecord, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal readonly struct ObjectStoreGetKeysInSlot : IScanIteratorFunctions<IGarnetObject>
            {
                readonly List<byte[]> keys;
                readonly int slot;

                internal ObjectStoreGetKeysInSlot(List<byte[]> keys, int slot)
                {
                    this.keys = keys;
                    this.slot = slot;
                }

                public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    var key = srcLogRecord.Key;
                    if (HashSlotUtils.HashSlot(key) == slot && !Expired<IGarnetObject, TSourceLogRecord>(ref srcLogRecord))
                        keys.Add(key.ToByteArray());
                    return true;
                }
                public bool ConcurrentReader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
                    => SingleReader(ref logRecord, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }
        }
    }
}