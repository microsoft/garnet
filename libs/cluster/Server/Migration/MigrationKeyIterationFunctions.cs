// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        internal sealed class MigrationKeyIterationFunctions
        {
            internal sealed unsafe class MainStoreGetKeysInSlots : IScanIteratorFunctions<SpanByte>
            {
                MigrationScanIterator iterator;

                internal MainStoreGetKeysInSlots(MigrateSession session, HashSet<int> slots, int bufferSize = 1 << 17)
                {
                    iterator = new MigrationScanIterator(session, slots, bufferSize);
                }

                internal void Dispose()
                {
                    iterator.Dispose();
                }

                public void AdvanceIterator() => iterator.AdvanceIterator();

                public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<SpanByte>
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                    // Do not send key if it is expired
                    if (ClusterSession.Expired<SpanByte, TSourceLogRecord>(ref srcLogRecord))
                        return true;

                    var key = srcLogRecord.Key.AsSpan();
                    var slot = HashSlotUtils.HashSlot(key);

                    // Transfer key if it belongs to slot that is currently being migrated
                    return !iterator.Contains(slot) || iterator.Consume(key);
                }

                public bool ConcurrentReader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<SpanByte>
                    => SingleReader(ref logRecord, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal struct ObjectStoreGetKeysInSlots : IScanIteratorFunctions<IGarnetObject>
            {
                MigrationScanIterator iterator;

                internal ObjectStoreGetKeysInSlots(MigrateSession session, HashSet<int> slots, int bufferSize = 1 << 17)
                {
                    iterator = new MigrationScanIterator(session, slots, bufferSize);
                }

                internal void Dispose()
                {
                    iterator.Dispose();
                }

                public void AdvanceIterator() => iterator.AdvanceIterator();

                public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                    // Do not send key if it is expired
                    if (ClusterSession.Expired<IGarnetObject, TSourceLogRecord>(ref srcLogRecord))
                        return true;

                    var key = srcLogRecord.Key.AsSpan();
                    var slot = HashSlotUtils.HashSlot(key);

                    // Transfer key if it belongs to slot that is currently being migrated
                    return !iterator.Contains(slot) || iterator.Consume(key);
                }

                public bool ConcurrentReader<TSourceLogRecord>(ref TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
                    => SingleReader(ref logRecord, recordMetadata, numberOfRecords, out cursorRecordResult);

                public bool OnStart(long beginAddress, long endAddress) => true;
                public void OnStop(bool completed, long numberOfRecords) { }
                public void OnException(Exception exception, long numberOfRecords) { }
            }

            internal sealed class MigrationScanIterator
            {
                readonly MigrateSession session;
                readonly HashSet<int> slots;

                readonly PoolEntry poolEntry;

                long offset;
                long currentOffset;
                byte[] keyBuffer;
                byte* endPtr;
                byte* currPtr;

                internal MigrationScanIterator(MigrateSession session, HashSet<int> slots, int bufferSize = 1 << 17)
                {
                    this.session = session;
                    this.slots = slots;
                    offset = 0;
                    currentOffset = 0;

                    poolEntry = session.GetNetworkPool.Get(size: bufferSize);
                    keyBuffer = poolEntry.entry;
                    currPtr = (byte*)Unsafe.AsPointer(ref keyBuffer[0]);
                    endPtr = (byte*)Unsafe.AsPointer(ref keyBuffer[^1]);
                }

                internal void Dispose()
                {
                    poolEntry.Dispose();
                }

                /// <summary>
                /// Check if slot is scheduled for migration
                /// </summary>
                /// <param name="slot"></param>
                /// <returns></returns>
                public bool Contains(int slot) => slots.Contains(slot);

                /// <summary>
                /// Advance iterator
                /// </summary>
                public void AdvanceIterator()
                {
                    // Update boundary of processing window
                    offset = currentOffset;
                    currentOffset = 0;
                    currPtr = (byte*)Unsafe.AsPointer(ref keyBuffer[0]);
                }

                /// <summary>
                /// Queue key for migration if there is enough space in the buffer
                /// </summary>
                /// <param name="key"></param>
                /// <returns></returns>
                public bool Consume(Span<byte> key)
                {
                    // Check if key is within the current processing window only if _copyOption is set
                    // in order to skip keys that have been send over to target node but not deleted locally
                    if (session._copyOption && currentOffset < offset)
                    {
                        currentOffset++;
                        return true;
                    }

                    // Create ArgSlice and check if there is enough space to copy current key
                    var keySlice = new ArgSlice(currPtr, key.Length);
                    if (currPtr + keySlice.Length > endPtr)
                        return false;

                    // Copy key to buffer and add it to migrate session dictionary
                    key.CopyTo(keySlice.Span);
                    if (!session.AddKey(ref keySlice))
                        throw new GarnetException("Failed to add migrating key to working set!");


                    // Move buffer ptr and key offset
                    currPtr += keySlice.Length;
                    currentOffset++;
                    return true;
                }
            }
        }
    }
}