﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession
    {
        #region mainStoreScan
        internal sealed unsafe class MainStoreScan : IScanIteratorFunctions
        {
            readonly MigrateOperation mss;

            internal MainStoreScan(MigrateOperation mss)
            {
                this.mss = mss;
            }

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }

            public void OnException(Exception exception, long numberOfRecords) { }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                mss.ThrowIfCancelled();

                // Do not send key if it is expired
                if (ClusterSession.Expired(in srcLogRecord))
                    return true;

                var key = srcLogRecord.Key;
                var slot = HashSlotUtils.HashSlot(key);

                // Check if key belongs to slot that is being migrated and if it can be added to our buffer
                if (mss.Contains(slot) && !mss.sketch.TryHashAndStore(key))
                    return false;

                return true;
            }
        }
        #endregion

        #region objectStoreScan
        internal sealed unsafe class ObjectStoreScan : IScanIteratorFunctions
        {
            readonly MigrateOperation mss;

            internal ObjectStoreScan(MigrateOperation mss)
            {
                this.mss = mss;
            }

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }

            public void OnException(Exception exception, long numberOfRecords) { }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                mss.ThrowIfCancelled();

                // Do not send key if it is expired
                if (ClusterSession.Expired(in srcLogRecord))
                    return true;

                var key = srcLogRecord.Key;
                var slot = HashSlotUtils.HashSlot(key);

                // Check if key belongs to slot that is being migrated and if it can be added to our buffer
                if (mss.Contains(slot) && !mss.sketch.TryHashAndStore(key))
                    return false;

                return true;
            }
        }
        #endregion
    }
}