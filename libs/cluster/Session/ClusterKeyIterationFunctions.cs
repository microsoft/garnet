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

            internal sealed class MainStoreCountKeys : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                private readonly KeyIterationInfo info;
                // This must be a class as it is passed through pending IO operations

                internal int KeyCount { get => info.keyCount; set => info.keyCount = value; }
                internal int Slot => info.slot;

                internal MainStoreCountKeys(int slot) => info = new(slot);

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    // TODO: better way to detect namespace
                    if (key.MetadataSize == 1)
                    {
                        // Namespace means not visible
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    if (HashSlotUtils.HashSlot(ref key) == Slot && !Expired(ref value))
                        KeyCount++;
                    return true;
                }
                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class ObjectStoreCountKeys : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                private readonly KeyIterationInfo info;
                // This must be a class as it is passed through pending IO operations

                internal int KeyCount { get => info.keyCount; set => info.keyCount = value; }
                internal int Slot => info.slot;

                internal ObjectStoreCountKeys(int slot) => info = new(slot);

                public bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here , out CursorRecordResult cursorRecordResult
                    fixed (byte* keyPtr = key)
                    {
                        if (HashSlotUtils.HashSlot(keyPtr, key.Length) == Slot && !Expired(ref value))
                            KeyCount++;
                    }
                    return true;
                }
                public bool ConcurrentReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal readonly struct MainStoreGetKeysInSlot : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                readonly List<byte[]> keys;
                readonly int slot, maxKeyCount;

                internal MainStoreGetKeysInSlot(List<byte[]> keys, int slot, int maxKeyCount)
                {
                    this.keys = keys;
                    this.slot = slot;
                    this.maxKeyCount = maxKeyCount;
                }

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    // TODO: better way to detect namespace
                    if (key.MetadataSize == 1)
                    {
                        // Namespace means not visible
                        cursorRecordResult = CursorRecordResult.Skip;
                        return true;
                    }

                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here, out CursorRecordResult cursorRecordResult

                    if (HashSlotUtils.HashSlot(ref key) == slot && !Expired(ref value))
                        keys.Add(key.ToByteArray());
                    return keys.Count < maxKeyCount;
                }
                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal readonly struct ObjectStoreGetKeysInSlot : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                readonly List<byte[]> keys;
                readonly int slot;

                internal ObjectStoreGetKeysInSlot(List<byte[]> keys, int slot)
                {
                    this.keys = keys;
                    this.slot = slot;
                }

                public bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    fixed (byte* keyPtr = key)
                    {
                        if (HashSlotUtils.HashSlot(keyPtr, key.Length) == slot && !Expired(ref value))
                            keys.Add(key);
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