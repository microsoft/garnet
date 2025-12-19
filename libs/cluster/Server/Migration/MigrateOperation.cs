// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
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

            private readonly ConcurrentDictionary<byte[], byte[]> vectorSetsIndexKeysToMigrate;

            readonly MigrateSession session;
            readonly GarnetClientSession gcs;
            readonly LocalServerSession localServerSession;

            public GarnetClientSession Client => gcs;

            public IEnumerable<KeyValuePair<byte[], byte[]>> VectorSets => vectorSetsIndexKeysToMigrate;

            public void ThrowIfCancelled() => session._cts.Token.ThrowIfCancellationRequested();

            public bool Contains(int slot) => session._sslots.Contains(slot);

            public bool ContainsNamespace(ulong ns) => session._namespaces?.Contains(ns) ?? false;

            public void EncounteredVectorSet(byte[] key, byte[] value)
            => vectorSetsIndexKeysToMigrate.TryAdd(key, value);

            public MigrateOperation(MigrateSession session, Sketch sketch = null, int batchSize = 1 << 18)
            {
                this.session = session;
                gcs = session.GetGarnetClient();
                localServerSession = session.GetLocalSession();
                this.sketch = sketch ?? new(keyCount: batchSize << 2);
                mss = new MainStoreScan(this);
                oss = new ObjectStoreScan(this);
                keysToDelete = [];
                vectorSetsIndexKeysToMigrate = new(ByteArrayComparer.Instance);
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
                            var spanByte = key;
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

            /// <summary>
            /// Move keys in sketch out of the given store, UNLESS they are also in <paramref name="vectorSetKeysToIgnore"/>.
            /// </summary>
            public bool TransmitKeys(StoreType storeType, Dictionary<byte[], byte[]> vectorSetKeysToIgnore)
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
#if NET9_0_OR_GREATER
                        var ignoreLookup = vectorSetKeysToIgnore.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif

                        for (var i = 0; i < keys.Count; i++)
                        {
                            if (keys[i].Item2)
                                continue;

                            var spanByte = keys[i].Item1.SpanByte;

                            // Don't transmit if a Vector Set
                            var isVectorSet =
                                vectorSetKeysToIgnore.Count > 0 &&
#if NET9_0_OR_GREATER
                                ignoreLookup.ContainsKey(spanByte.AsReadOnlySpan());
#else
                                vectorSetKeysToIgnore.ContainsKey(spanByte.ToByteArray());
#endif
                            if (isVectorSet)
                            {
                                continue;
                            }

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

                            var spanByte = keys[i].Item1.SpanByte;
                            if (!session.WriteOrSendObjectStoreKeyValuePair(gcs, localServerSession, ref spanByte, out var status))
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
            /// Transmit data in namespaces during a MIGRATE ... KEYS operation.
            /// 
            /// Doesn't delete anything, just scans and transmits.
            /// </summary>
            public bool TransmitKeysNamespaces(ILogger logger)
            {
                var migrateOperation = this;

                if (!migrateOperation.Initialize())
                    return false;

                var workerStartAddress = migrateOperation.session.clusterProvider.storeWrapper.store.Log.BeginAddress;
                var workerEndAddress = migrateOperation.session.clusterProvider.storeWrapper.store.Log.TailAddress;

                var cursor = workerStartAddress;
                logger?.LogWarning("<MainStore> migrate keys (namespaces) scan range [{workerStartAddress}, {workerEndAddress}]", workerStartAddress, workerEndAddress);
                while (true)
                {
                    var current = cursor;
                    // Build Sketch
                    migrateOperation.sketch.SetStatus(SketchStatus.INITIALIZING);
                    migrateOperation.Scan(StoreType.Main, ref current, workerEndAddress);

                    // Stop if no keys have been found
                    if (migrateOperation.sketch.argSliceVector.IsEmpty) break;

                    logger?.LogWarning("Scan from {cursor} to {current} and discovered {count} keys", cursor, current, migrateOperation.sketch.argSliceVector.Count);

                    // Transition EPSM to MIGRATING
                    migrateOperation.sketch.SetStatus(SketchStatus.TRANSMITTING);
                    migrateOperation.session.WaitForConfigPropagation();

                    // Transmit all keys gathered
                    migrateOperation.TransmitSlots(StoreType.Main);

                    // Transition EPSM to DELETING
                    migrateOperation.sketch.SetStatus(SketchStatus.DELETING);
                    migrateOperation.session.WaitForConfigPropagation();

                    // Clear keys from buffer
                    migrateOperation.sketch.Clear();
                    cursor = current;
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
                        if (key.MetadataSize == 1)
                        {
                            // Namespace'd keys are not deleted here, but when migration finishes
                            continue;
                        }

                        var spanByte = key;
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

            /// <summary>
            /// Delete a Vector Set after migration if _copyOption is not set.
            /// </summary>
            public void DeleteVectorSet(ref SpanByte key)
            {
                if (session._copyOption)
                    return;

                var delRes = localServerSession.BasicGarnetApi.DELETE(ref key);

                session.logger?.LogDebug("Deleting Vector Set {key} after migration: {delRes}", System.Text.Encoding.UTF8.GetString(key.AsReadOnlySpan()), delRes);
            }
        }
    }
}