// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession
    {
        internal sealed unsafe class StoreScan : IScanIteratorFunctions
        {
            readonly MigrateOperation migrateOperation;

            internal StoreScan(MigrateOperation migrateOperation)
            {
                this.migrateOperation = migrateOperation;
            }

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }

            public void OnException(Exception exception, long numberOfRecords) { }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                migrateOperation.ThrowIfCancelled();

                // Do not send key if it is expired
                if (!srcLogRecord.Info.Tombstone && ClusterSession.Expired(in srcLogRecord))
                    return true;

                if (srcLogRecord.HasNamespace)
                {
                    // Migrating a Vector Set element
                    if (migrateOperation.ContainsNamespace(srcLogRecord.NamespaceBytes) && !migrateOperation.sketch.TryHashAndStore(srcLogRecord.NamespaceBytes, srcLogRecord.KeyBytes))
                        return false;
                }
                else
                {
                    var key = srcLogRecord.Key;
                    var slot = HashSlotUtils.HashSlot(key);

                    // Check if key belongs to slot that is being migrated and if it can be added to our buffer
                    if (migrateOperation.Contains(slot))
                    {
                        if (srcLogRecord.RecordType == VectorManager.RecordType)
                        {
                            // We can't delete the vector set _yet_ nor can we migrate it, 
                            // we just need to remember it to migrate once the associated namespaces are all moved over
                            migrateOperation.EncounteredVectorSet(key.ToArray(), srcLogRecord.ValueSpan.ToArray());
                        }
                        else if (!migrateOperation.sketch.TryHashAndStore(key))
                        {
                            return false;
                        }
                    }
                }

                return true;
            }
        }
    }
}