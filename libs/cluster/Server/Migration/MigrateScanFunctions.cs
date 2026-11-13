// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession
    {
        internal sealed unsafe class StoreScan<TFilter> : IScanIteratorFunctions
            where TFilter : struct, IBloomFilter
        {
            readonly MigrateOperation<TFilter> migrateOperation;
            readonly ArgSliceVector destination;
            readonly bool keysOnly;
            readonly CancellationToken cancellationToken;

            internal StoreScan(MigrateOperation<TFilter> migrateOperation, ArgSliceVector destination = null, CancellationToken cancellationToken = default)
            {
                this.migrateOperation = migrateOperation;
                this.destination = destination;
                keysOnly = destination != null;
                this.cancellationToken = cancellationToken;
            }

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool IncludeValues => !keysOnly;

            public void OnStop(bool completed, long numberOfRecords) { }

            public void OnException(Exception exception, long numberOfRecords) { }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;

                migrateOperation.ThrowIfCancelled();
                cancellationToken.ThrowIfCancellationRequested();
                var sketch = migrateOperation.sketch;

                // Do not send key if it is expired
                if (!srcLogRecord.Info.Tombstone && ClusterSession.Expired(in srcLogRecord))
                    return true;

                if (srcLogRecord.HasNamespace)
                {
                    // Migrating a Vector Set element
                    if (migrateOperation.ContainsNamespace(srcLogRecord.NamespaceBytes) &&
                        !(keysOnly ? destination.TryAddItem(srcLogRecord.NamespaceBytes, srcLogRecord.KeyBytes)
                            : sketch.TryHashAndStore(srcLogRecord.NamespaceBytes, srcLogRecord.KeyBytes)))
                    {
                        cursorRecordResult = CursorRecordResult.EndBatch | CursorRecordResult.RetryLastRecord;
                        return true;
                    }
                }
                else
                {
                    var key = srcLogRecord.Key;
                    var slot = HashSlotUtils.HashSlot(key);

                    // Check if key belongs to slot that is being migrated and if it can be added to our buffer
                    if (migrateOperation.Contains(slot))
                    {
                        if (!keysOnly && srcLogRecord.RecordType == RangeIndexManager.RangeIndexRecordType)
                        {
                            // RangeIndex keys need out-of-band migration (snapshot + chunks)
                            migrateOperation.AddRangeIndexKey(key.ToArray());
                        }
                        else if (!keysOnly && srcLogRecord.RecordType == VectorManager.RecordType)
                        {
                            // We can't delete the vector set _yet_ nor can we migrate it, 
                            // we just need to remember it to migrate once the associated namespaces are all moved over

                            // Track expiration (if any) as a concrete date rather than offset since it may be a bit before we shove the bytes across the wire
                            DateTime? expiration = srcLogRecord.DataHeader.HasExpiration ? new DateTime(srcLogRecord.Expiration, DateTimeKind.Utc) : null;
                            migrateOperation.EncounteredVectorSet(key.ToArray(), srcLogRecord.ValueSpan.ToArray(), expiration);
                        }
                        else if (!(keysOnly ? destination.TryAddItem(key) : sketch.TryHashAndStore(key)))
                        {
                            cursorRecordResult = CursorRecordResult.EndBatch | CursorRecordResult.RetryLastRecord;
                            return true;
                        }
                    }
                }

                return true;
            }
        }
    }
}