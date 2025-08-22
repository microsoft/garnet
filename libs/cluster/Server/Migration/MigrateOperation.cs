// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.client;
using Garnet.server;

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
            public bool TransmitSlots(StoreType storeType)
            {
                // Use this for both stores; main store will just use the SpanByteAndMemory directly. We want it to be outside iterations
                // so we can reuse the SpanByteAndMemory.Memory across iterations.
                var output = new GarnetObjectStoreOutput();

                try
                {
                    if (storeType == StoreType.Main)
                    {
                        var input = new RawStringInput(RespCommandAccessor.MIGRATE);
                        foreach (var key in sketch.argSliceVector)
                        {
                            if (!session.WriteOrSendMainStoreKeyValuePair(gcs, localServerSession, key, ref input, ref output.SpanByteAndMemory, out _))
                                return false;
                        }
                    }
                    else
                    {
                        var input = new ObjectInput(new RespInputHeader(GarnetObjectType.Migrate));
                        foreach (var key in sketch.argSliceVector)
                        {
                            if (!session.WriteOrSendObjectStoreKeyValuePair(gcs, localServerSession, key, ref input, ref output, out _))
                                return false;
                        }
                    }

                    // Flush final data in client buffer
                    if (!session.HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                        return false;
                }
                finally
                {
                    output.SpanByteAndMemory.Dispose();
                }

                return true;
            }

            public bool TransmitKeys(StoreType storeType)
            {
                // Use this for both stores; main store will just use the SpanByteAndMemory directly. We want it to be outside iterations
                // so we can reuse the SpanByteAndMemory.Memory across iterations.
                var output = new GarnetObjectStoreOutput();

                try
                {
                    var keys = sketch.Keys;
                    if (storeType == StoreType.Main)
                    {
                        var input = new RawStringInput(RespCommandAccessor.MIGRATE);

                        for (var i = 0; i < keys.Count; i++)
                        {
                            if (keys[i].Item2)
                                continue;

                            if (!session.WriteOrSendMainStoreKeyValuePair(gcs, localServerSession, keys[i].Item1, ref input, ref output.SpanByteAndMemory, out var status))
                                return false;

                            // If key was FOUND, mark it for deletion
                            if (status != GarnetStatus.NOTFOUND)
                                keys[i] = (keys[i].Item1, true);
                        }
                    }
                    else
                    {
                        var input = new ObjectInput(new RespInputHeader(GarnetObjectType.Migrate));

                        for (var i = 0; i < keys.Count; i++)
                        {
                            if (keys[i].Item2)
                                continue;

                            if (!session.WriteOrSendObjectStoreKeyValuePair(gcs, localServerSession, keys[i].Item1, ref input, ref output, out var status))
                                return false;

                            // If key was FOUND, mark it for deletion
                            if (status != GarnetStatus.NOTFOUND)
                                keys[i] = (keys[i].Item1, true);
                        }
                    }

                    // Flush final data in client buffer
                    if (!session.HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                        return false;
                }
                finally
                {
                    output.SpanByteAndMemory.Dispose();
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
                        _ = localServerSession.BasicGarnetApi.DELETE(key);
                }
                else
                {
                    var keys = sketch.Keys;
                    for (var i = 0; i < keys.Count; i++)
                    {
                        // Do not delete the key if it is not marked for deletion because it has not been transmitted to the target node
                        if (keys[i].Item2)
                            _ = localServerSession.BasicGarnetApi.DELETE(keys[i].Item1);
                    }
                }
            }
        }
    }
}