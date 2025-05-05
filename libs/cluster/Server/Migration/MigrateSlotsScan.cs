// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal enum MigratePhase : byte
    {
        BuildSketch,
        TransmitData,
        DeletingData
    }

    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        internal class MigrateScan
        {
            public readonly MigratingKeysSketch sketch;
            public readonly List<byte[]> keysToDelete;
            public MainStoreScan mss;
            public ObjectStoreScan oss;

            readonly MigrateSession session;
            readonly GarnetClientSession gcs;
            MigratePhase phase;
            readonly int batchSize;

            bool initialized = false;
            readonly ILogger logger;

            public long Count { get; private set; }

            public MigrateScan(MigrateSession session, MigratingKeysSketch sketch = null, int batchSize = 1 << 18, ILogger logger = null)
            {
                this.session = session;
                gcs = session.GetGarnetClient();
                this.sketch = sketch ?? new(size: batchSize << 2);
                this.logger = logger;
                mss = new MainStoreScan(this);
                oss = new ObjectStoreScan(this);
                keysToDelete = [];
                this.batchSize = batchSize;
            }

            public void Initialize()
            {
                if (initialized) return;
                gcs.Connect();
                gcs.InitializeIterationBuffer(session.clusterProvider.storeWrapper.loggingFrequency);
                initialized = true;
            }

            public bool IsFullBatch() => Count > batchSize;

            public void Dispose()
            {
                gcs.Dispose();
            }

            public void SetPhase(MigratePhase phase) => this.phase = phase;

            public bool Probe(ref ArgSlice key, out KeyMigrationStatus status)
            {
                var spanByte = key.SpanByte;
                return Probe(ref spanByte, out status);
            }

            public bool Probe(ref SpanByte key, out KeyMigrationStatus status) => sketch.Probe(ref key, out status);

            public void SetKeysStatus(KeyMigrationStatus status) => sketch.SetStatus(status);

            #region commonScanMethods
            public bool OnStart(long beginAddress, long endAddress, StoreType storeType)
            {
                logger?.LogTrace("[{ScanClassName}] {storeType} {phase} {beginAddress} {endAddress}",
                    nameof(MigrateScan), storeType, phase, beginAddress, endAddress);
                return true;
            }

            public void OnStop(bool completed, long numberOfRecords, StoreType storeType)
            {
                logger?.LogTrace("[{ScanClassName}] {storeType} {phase} {numberOfRecords}",
                    nameof(MigrateScan), storeType, phase, numberOfRecords);
                if (phase == MigratePhase.TransmitData)
                {
                    _ = session.HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer());
                    _ = session.HandleMigrateTaskResponse(gcs.CompleteMigrate(session._sourceNodeId, session._replaceOption, isMainStore: storeType == StoreType.Main));
                    Count = 0;
                }
            }

            public void OnException(Exception exception, long numberOfRecords)
                => logger?.LogError("[{ScanClassName}] {ex}", nameof(MigrateScan), exception.Message);
            #endregion

            #region mainStoreScan
            internal sealed unsafe class MainStoreScan : IScanIteratorFunctions<SpanByte, SpanByte>
            {
                readonly MigrateScan mss;

                internal MainStoreScan(MigrateScan mss)
                {
                    this.mss = mss;
                }

                public bool OnStart(long beginAddress, long endAddress) => mss.OnStart(beginAddress, endAddress, StoreType.Main);

                public void OnStop(bool completed, long numberOfRecords) => mss.OnStop(completed, numberOfRecords, StoreType.Main);

                public void OnException(Exception exception, long numberOfRecords) => mss.OnException(exception, numberOfRecords);

                public unsafe bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                    // Do not send key if it is expired
                    if (ClusterSession.Expired(ref value))
                        return true;

                    var s = HashSlotUtils.HashSlot(ref key);
                    // Check if key belongs to slot that is being migrated
                    if (mss.session._sslots.Contains(s))
                    {
                        // Add to sketch
                        switch (mss.phase)
                        {
                            case MigratePhase.BuildSketch:
                                if (mss.IsFullBatch()) return false;
                                mss.sketch.Hash(key.ToPointer(), key.Length);
                                mss.Count++;
                                break;
                            case MigratePhase.TransmitData:
                                if (!mss.session.WriteOrSendMainStoreKeyValuePair(mss.gcs, ref key, ref value))
                                {
                                    mss.logger?.LogError("{ScanClass} failed to write or flush network buffer.", nameof(MainStoreScan));
                                    return false;
                                }
                                break;
                            case MigratePhase.DeletingData:
                                mss.keysToDelete.Add(key.AsReadOnlySpan().ToArray());
                                break;
                        }
                    }

                    return true;
                }

                public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
            }
            #endregion

            #region objectStoreScan
            internal sealed unsafe class ObjectStoreScan : IScanIteratorFunctions<byte[], IGarnetObject>
            {
                readonly MigrateScan mss;

                internal ObjectStoreScan(MigrateScan mss)
                {
                    this.mss = mss;
                }

                public bool OnStart(long beginAddress, long endAddress) => mss.OnStart(beginAddress, endAddress, StoreType.Object);

                public void OnStop(bool completed, long numberOfRecords) => mss.OnStop(completed, numberOfRecords, StoreType.Object);

                public void OnException(Exception exception, long numberOfRecords) => mss.OnException(exception, numberOfRecords);

                public bool ConcurrentReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                    => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

                public unsafe bool SingleReader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                {
                    cursorRecordResult = CursorRecordResult.Accept; // default; not used here

                    // Do not send key if it is expired
                    if (ClusterSession.Expired(ref value))
                        return true;

                    var s = HashSlotUtils.HashSlot(key);
                    // Check if key belongs to slot that is being migrated
                    if (mss.session._sslots.Contains(s))
                    {
                        // Add to sketch
                        switch (mss.phase)
                        {
                            case MigratePhase.BuildSketch:
                                if (mss.IsFullBatch()) return false;
                                fixed (byte* keyPtr = key)
                                    mss.sketch.Hash(keyPtr, key.Length);
                                mss.Count++;
                                break;
                            case MigratePhase.TransmitData:
                                var objectData = GarnetObjectSerializer.Serialize(value);
                                if (!mss.session.WriteOrSendObjectStoreKeyValuePair(mss.gcs, key, objectData, value.Expiration))
                                {
                                    mss.logger?.LogError("{ScanClass} failed to write or flush network buffer.", nameof(ObjectStoreScan));
                                    return false;
                                }
                                break;
                            case MigratePhase.DeletingData:
                                mss.keysToDelete.Add(key);
                                break;
                        }
                    }

                    return true;
                }
            }
            #endregion
        }
    }
}