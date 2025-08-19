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
        #region mainStoreScan
        internal sealed unsafe class MainStoreScan : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            readonly MigrateOperation mss;

            internal MainStoreScan(MigrateOperation mss)
            {
                this.mss = mss;
            }

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }

            public void OnException(Exception exception, long numberOfRecords) { }

            public unsafe bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                mss.ThrowIfCancelled();

                // Do not send key if it is expired
                if (ClusterSession.Expired(ref value))
                    return true;

                var s = HashSlotUtils.HashSlot(ref key);
                // Check if key belongs to slot that is being migrated and if it can be added to our buffer
                if (mss.Contains(s) && !mss.sketch.TryHashAndStore(key.AsSpan()))
                    return false;

                return true;
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
        }
        #endregion

        #region objectStoreScan
        internal sealed unsafe class ObjectStoreScan : IScanIteratorFunctions<byte[], IGarnetObject>
        {
            readonly MigrateOperation mss;

            internal ObjectStoreScan(MigrateOperation mss)
            {
                this.mss = mss;
            }

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }

            public void OnException(Exception exception, long numberOfRecords) { }

            public bool ConcurrentReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public unsafe bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                mss.ThrowIfCancelled();

                // Do not send key if it is expired
                if (ClusterSession.Expired(ref value))
                    return true;

                var s = HashSlotUtils.HashSlot(key);
                // Check if key belongs to slot that is being migrated and if it can be added to our buffer
                if (mss.Contains(s) && !mss.sketch.TryHashAndStore(key.AsSpan()))
                    return false;

                return true;
            }
        }
        #endregion
    }
}