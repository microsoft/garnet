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
        internal sealed partial class MigrateTask
        {
            public readonly Sketch sketch;
            public readonly List<byte[]> keysToDelete;
            public MainStoreScan mss;
            public ObjectStoreScan oss;

            readonly MigrateSession session;
            readonly GarnetClientSession gcs;
            readonly LocalServerSession localServerSession;

            bool initialized = false;

            public bool Contains(int slot) => session._sslots.Contains(slot);

            public MigrateTask(MigrateSession session, Sketch sketch = null, int batchSize = 1 << 18)
            {
                this.session = session;
                gcs = session.GetGarnetClient();
                localServerSession = session.GetLocalSession();
                this.sketch = sketch ?? new(keyCount: batchSize << 2);
                mss = new MainStoreScan(this);
                oss = new ObjectStoreScan(this);
                keysToDelete = [];
            }

            public void Initialize()
            {
                if (initialized) return;
                gcs.Connect();
                gcs.InitializeIterationBuffer(session.clusterProvider.storeWrapper.loggingFrequency);
                initialized = true;
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
                    _ = localServerSession.BasicGarnetApi.IterateMainStore(ref mss, ref currentAddress, endAddress, endAddress, returnTombstoned: true);
                else if (storeType == StoreType.Object)
                    _ = localServerSession.BasicGarnetApi.IterateObjectStore(ref oss, ref currentAddress, endAddress, endAddress, returnTombstoned: true);
            }

            /// <summary>
            /// Transmit gathered keys
            /// </summary>
            /// <param name="storeType"></param>
            /// <returns></returns>
            public bool Transmit(StoreType storeType)
            {
                var bufferSize = 1 << 10;
                SectorAlignedMemory buffer = new(bufferSize, 1);
                var bufPtr = buffer.GetValidPointer();
                var bufPtrEnd = bufPtr + bufferSize;
                var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                var input = new RawStringInput(RespCommandAccessor.MIGRATE);

                try
                {
                    foreach (var key in sketch.argSliceVector)
                    {
                        if (storeType == StoreType.Main)
                        {
                            var spanByte = key.SpanByte;
                            if (!session.WriteOrSendMainStoreKeyValuePair(gcs, localServerSession, ref spanByte, ref input, ref o))
                                return false;

                            // Reset SpanByte for next read if any but don't dispose heap buffer as we might re-use it
                            o.SpanByte = new SpanByte((int)(bufPtrEnd - bufPtr), (IntPtr)bufPtr);
                        }
                        else if (storeType == StoreType.Object)
                        {
                            var argSlice = key;
                            if (!session.WriteOrSendObjectStoreKeyValuePair(gcs, localServerSession, ref argSlice, ref o))
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

            /// <summary>
            /// Delete keys after migration if copyOption is not set
            /// </summary>
            public void DeleteKeys()
            {
                if (session._copyOption)
                    return;
                foreach (var key in sketch.argSliceVector)
                {
                    var spanByte = key.SpanByte;
                    _ = localServerSession.BasicGarnetApi.DELETE(ref spanByte);
                }
            }
        }
    }
}