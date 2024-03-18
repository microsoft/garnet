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
            internal struct MainStoreCountKeys : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                internal int keyCount;
                readonly int slot;

                internal MainStoreCountKeys(int slot) => this.slot = slot;

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    if (NumUtils.HashSlot(key.ToPointer(), key.LengthWithoutMetadata) == slot && !Expired(ref value))
                        keyCount++;
                    return true;
                }
                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal struct ObjectStoreCountKeys : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                internal int keyCount;
                readonly int slot;

                internal ObjectStoreCountKeys(int slot) => this.slot = slot;

                public bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here , out CursorRecordResult cursorRecordResult
                    fixed (byte* keyPtr = key)
                    {
                        if (NumUtils.HashSlot(keyPtr, key.Length) == slot && !Expired(ref value))
                            keyCount++;
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
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here, out CursorRecordResult cursorRecordResult
                    if (NumUtils.HashSlot(key.ToPointer(), key.LengthWithoutMetadata) == slot && !Expired(ref value))
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
                        if (NumUtils.HashSlot(keyPtr, key.Length) == slot && !Expired(ref value))
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