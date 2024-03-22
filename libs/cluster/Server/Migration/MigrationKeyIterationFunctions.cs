// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        internal class MigrationKeyIterationFunctions
        {
            internal struct MainStoreMigrateSlots : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                readonly MigrateSession session;
                readonly HashSet<int> slots;

                internal MainStoreMigrateSlots(MigrateSession session, HashSet<int> slots)
                {
                    this.session = session;
                    this.slots = slots;
                }

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    var s = NumUtils.HashSlot(key.ToPointer(), key.Length);

                    if (slots.Contains(s) && !ClusterSession.Expired(ref value) && !session.WriteOrSendMainStoreKeyValuePair(ref key, ref value))
                        return false;
                    return true;
                }
                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal struct ObjectStoreMigrateSlots : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                readonly MigrateSession session;
                readonly HashSet<int> slots;

                internal ObjectStoreMigrateSlots(MigrateSession session, HashSet<int> slots)
                {
                    this.session = session;
                    this.slots = slots;
                }

                public bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    var slot = NumUtils.HashSlot(key);

                    if (slots.Contains(slot) && !ClusterSession.Expired(ref value))
                    {
                        byte[] objectData = session.clusterProvider.storeWrapper.SerializeGarnetObject(value);
                        if (!session.WriteOrSendObjectStoreKeyValuePair(key, objectData, value.Expiration))
                            return false;
                    }
                    return true;
                }
                public bool ConcurrentReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal readonly struct MainStoreDeleteKeysInSlot : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session;
                readonly HashSet<int> slots;

                internal MainStoreDeleteKeysInSlot(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session, HashSet<int> slots)
                {
                    this.session = session;
                    this.slots = slots;
                }

                public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                    var s = NumUtils.HashSlot(key.ToPointer(), key.Length);
                    if (slots.Contains(s))
                        session.Delete(key);
                    return true;
                }

                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }
        }
    }
}