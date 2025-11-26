// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.client;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        internal sealed partial class MigrateOperation
        {
            public readonly Sketch sketch;
            public readonly List<byte[]> keysToDelete;
            public MainStoreScan mss;
            public ObjectStoreScan oss;

            readonly MigrateSession session;
            readonly GarnetClientSession gcs;
            readonly LocalServerSession localServerSession;

            public GarnetClientSession Client => gcs;

            public void ThrowIfCancelled() => session._cts.Token.ThrowIfCancellationRequested();

            public bool Contains(int slot) => session._sslots.Contains(slot);

            public MigrateOperation(MigrateSession session, Sketch sketch = null, int batchSize = 1 << 18)
            {
                this.session = session;
                gcs = session.GetGarnetClient();
                localServerSession = session.GetLocalSession();
                this.sketch = sketch ?? new(keyCount: batchSize << 2);
                mss = new MainStoreScan(this);
                oss = new ObjectStoreScan(this);
                keysToDelete = [];
            }

            public bool Initialize()
            {
                if (!session.CheckConnection(gcs))
                    return false;
                gcs.InitializeIterationBuffer(session.clusterProvider.storeWrapper.loggingFrequency);
                return true;
            }

            public void Dispose()
            {
                gcs.Dispose();
                localServerSession.Dispose();
            }

            /// <summary>
            /// Perform scan to gather keys and build sketch
            /// </summary>
            /// <param name="storeType"></param>
            /// <param name="currentAddress"></param>
            /// <param name="endAddress"></param>
            public void Scan(StoreType storeType, ref long currentAddress, long endAddress)
            {
                if (storeType == StoreType.Main)
                    _ = localServerSession.BasicGarnetApi.IterateMainStore(ref mss, ref currentAddress, endAddress, endAddress, includeTombstones: true);
                else if (storeType == StoreType.Object)
                    _ = localServerSession.BasicGarnetApi.IterateObjectStore(ref oss, ref currentAddress, endAddress, endAddress, includeTombstones: true);
            }

            /// <summary>
            /// Transmit gathered keys
            /// </summary>
            /// <param name="storeType"></param>
            /// <returns></returns>
            public bool TrasmitSlots(StoreType storeType)
            {
                var bufferSize = 1 << 10;
                SectorAlignedMemory buffer = new(bufferSize, 1);
                var bufPtr = buffer.GetValidPointer();
                var bufPtrEnd = bufPtr + bufferSize;
                var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                var input = new RawStringInput(RespCommandAccessor.MIGRATE);

                try
                {
                    if (storeType == StoreType.Main)
                    {
                        foreach (var key in sketch.argSliceVector)
                        {
                            var spanByte = key.SpanByte;
                            if (!session.WriteOrSendMainStoreKeyValuePair(gcs, localServerSession, ref spanByte, ref input, ref o, out _))
                                return false;

                            // Reset SpanByte for next read if any but don't dispose heap buffer as we might re-use it
                            o.SpanByte = new SpanByte((int)(bufPtrEnd - bufPtr), (IntPtr)bufPtr);
                        }
                    }
                    else
                    {
                        foreach (var key in sketch.argSliceVector)
                        {
                            var argSlice = key;
                            if (!session.WriteOrSendObjectStoreKeyValuePair(gcs, localServerSession, ref argSlice, out _))
                                return false;
                        }
                    }

                    // Flush final data in client buffer
                    if (!session.HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                        return false;
                }
                finally
                {
                    buffer.Dispose();
                }

                return true;
            }

            public bool TransmitKeys(StoreType storeType)
            {
                var bufferSize = 1 << 10;
                SectorAlignedMemory buffer = new(bufferSize, 1);
                var bufPtr = buffer.GetValidPointer();
                var bufPtrEnd = bufPtr + bufferSize;
                var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                var input = new RawStringInput(RespCommandAccessor.MIGRATE);

                try
                {
                    var keys = sketch.Keys;
                    if (storeType == StoreType.Main)
                    {
                        for (var i = 0; i < keys.Count; i++)
                        {
                            if (keys[i].Item2)
                                continue;

                            var spanByte = keys[i].Item1.SpanByte;
                            if (!session.WriteOrSendMainStoreKeyValuePair(gcs, localServerSession, ref spanByte, ref input, ref o, out var status))
                                return false;

                            // Skip if key NOTFOUND
                            if (status == GarnetStatus.NOTFOUND)
                                continue;

                            // Reset SpanByte for next read if any but don't dispose heap buffer as we might re-use it
                            o.SpanByte = new SpanByte((int)(bufPtrEnd - bufPtr), (IntPtr)bufPtr);

                            // Mark for deletion
                            keys[i] = (keys[i].Item1, true);
                        }
                    }
                    else
                    {
                        for (var i = 0; i < keys.Count; i++)
                        {
                            if (keys[i].Item2)
                                continue;

                            var argSlice = keys[i].Item1;
                            if (!session.WriteOrSendObjectStoreKeyValuePair(gcs, localServerSession, ref argSlice, out var status))
                                return false;

                            // Skip if key NOTFOUND
                            if (status == GarnetStatus.NOTFOUND)
                                continue;

                            // Mark for deletion
                            keys[i] = (keys[i].Item1, true);
                        }
                    }

                    // Flush final data in client buffer
                    if (!session.HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                        return false;
                }
                finally
                {
                    buffer.Dispose();
                }
                return true;
            }

            /// <summary>
            /// Delete keys after migration if copyOption is not set
            /// </summary>
            public void DeleteKeys()
            {
                if (session._copyOption)
                    return;
                if (session.transferOption == TransferOption.SLOTS)
                {
                    foreach (var key in sketch.argSliceVector)
                    {
                        var spanByte = key.SpanByte;
                        _ = localServerSession.BasicGarnetApi.DELETE(ref spanByte);
                    }
                }
                else
                {
                    var keys = sketch.Keys;
                    for (var i = 0; i < keys.Count; i++)
                    {
                        // Skip if key is not marked for deletion because it has not been transmitted to the target node
                        if (!keys[i].Item2) continue;
                        var spanByte = keys[i].Item1.SpanByte;
                        _ = localServerSession.BasicGarnetApi.DELETE(ref spanByte);
                    }
                }
            }
        }
    }
}