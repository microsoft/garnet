// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        internal sealed partial class MigrateOperation
        {
            public readonly Sketch sketch;
            public readonly List<byte[]> keysToDelete;
            public StoreScan storeScan;

            private readonly ConcurrentDictionary<byte[], byte[]> vectorSetsIndexKeysToMigrate;

            readonly MigrateSession session;
            readonly GarnetClientSession gcs;
            readonly LocalServerSession localServerSession;

            public GarnetClientSession Client => gcs;

            public IEnumerable<KeyValuePair<byte[], byte[]>> VectorSets => vectorSetsIndexKeysToMigrate;

            public void ThrowIfCancelled() => session._cts.Token.ThrowIfCancellationRequested();

            public bool Contains(int slot) => session._sslots.Contains(slot);

            public bool ContainsNamespace(ReadOnlySpan<byte> namespaceBytes)
            {
                Debug.Assert(namespaceBytes.Length == 1, "Longer namespaces note supported");

                var ns = (ulong)namespaceBytes[0];

                return session._namespaces?.Contains(ns) ?? false;
            }
            public void EncounteredVectorSet(byte[] key, byte[] value)
            => vectorSetsIndexKeysToMigrate.TryAdd(key, value);

            public MigrateOperation(MigrateSession session, Sketch sketch = null, int batchSize = 1 << 18)
            {
                this.session = session;
                gcs = session.GetGarnetClient();
                localServerSession = session.GetLocalSession();
                this.sketch = sketch ?? new(keyCount: batchSize << 2);
                storeScan = new StoreScan(this);
                keysToDelete = [];
                vectorSetsIndexKeysToMigrate = new(ByteArrayComparer.Instance);
            }

            public async ValueTask<bool> InitializeAsync()
            {
                if (!await session.CheckConnectionAsync(gcs).ConfigureAwait(false))
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
            /// <param name="currentAddress"></param>
            /// <param name="endAddress"></param>
            public void Scan(ref long currentAddress, long endAddress)
                => localServerSession.BasicGarnetApi.IterateStore(ref storeScan, ref currentAddress, endAddress, endAddress,
                    includeTombstones: true);

            /// <summary>
            /// Transmit gathered keys
            /// </summary>
            /// <returns></returns>
            public async Task<bool> TransmitSlotsAsync()
            {
                var output = new UnifiedOutput();       // TODO: initialize this based on gcs curr and end; make sure it has the initial part of the "send" set
                var vectorOutput = new VectorOutput();  // TODO: initialize this based on gcs curr and end; make sure it has the initial part of the "send" set

                try
                {
                    var input = new UnifiedInput(RespCommand.MIGRATE);
                    input.arg1 = session.NetworkBufferSettings.sendBufferSize - common.NetworkBufferSettings.SendBufferOverheadReserve;

                    VectorInput vectorInput = new();
                    vectorInput.AlignmentExpected = true; // We're moving DiskANN sourced data, so alignment is expected
                    vectorInput.MaxMigrationHeapAllocationSize = session.NetworkBufferSettings.sendBufferSize - common.NetworkBufferSettings.SendBufferOverheadReserve;

                    foreach (var (ns, key, hasNs) in sketch.argSliceVector)
                    {
                        if (hasNs)
                        {
                            // Migrating Vector Set element data
                            if (!await session.WriteOrSendRecordAsync(gcs, localServerSession, ns, key, ref vectorInput, ref vectorOutput, out _).ConfigureAwait(false))
                                return false;
                        }
                        else
                        {
                            // Migrating everything else
                            if (!await session.WriteOrSendRecordAsync(gcs, localServerSession, key, ref input, ref output, out _).ConfigureAwait(false))
                                return false;
                        }
                    }

                    // Flush final data in client buffer
                    if (!await session.HandleMigrateTaskResponseAsync(gcs.SendAndResetIterationBuffer()).ConfigureAwait(false))
                        return false;
                }
                finally
                {
                    output.SpanByteAndMemory.Dispose();
                    vectorOutput.SpanByteAndMemory.Dispose();
                }

                return true;
            }

            public async Task<bool> TransmitKeysAsync(Dictionary<byte[], byte[]> vectorSetKeysToIgnore)
            {
                // Use this for both stores; main store will just use the SpanByteAndMemory directly. We want it to be outside iterations
                // so we can reuse the SpanByteAndMemory.Memory across iterations.
                // TODO: initialize 'output' based on gcs curr and end; make sure it has the initial part of the "send" set, and call gcs.IncrementRecordDirect().
                //       This will still allow SBAM.Memory to be reused.
                var output = new UnifiedOutput();

#if NET9_0_OR_GREATER
                var ignoreLookup = vectorSetKeysToIgnore.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif

                try
                {
                    var keys = sketch.Keys;

                    var input = new UnifiedInput(RespCommand.MIGRATE)
                    {
                        arg1 = session.NetworkBufferSettings.sendBufferSize - 1024   // Reserve some space for overhead
                    };
                    for (var i = 0; i < keys.Count; i++)
                    {
                        if (keys[i].Item2)
                            continue;

                        var spanByte = keys[i].Item1;

                        // Don't transmit if a Vector Set
                        var isVectorSet =
                            vectorSetKeysToIgnore.Count > 0 &&
#if NET9_0_OR_GREATER
                            ignoreLookup.ContainsKey(spanByte.ReadOnlySpan);
#else
                                vectorSetKeysToIgnore.ContainsKey(spanByte.ToArray());
#endif

                        if (isVectorSet)
                        {
                            continue;
                        }

                        if (!await session.WriteOrSendRecordAsync(gcs, localServerSession, keys[i].Item1, ref input, ref output, out var status).ConfigureAwait(false))
                            return false;

                        // If key was FOUND, mark it for deletion
                        if (status != GarnetStatus.NOTFOUND)
                            keys[i] = (keys[i].Item1, true);
                    }

                    // Flush final data in client buffer
                    if (!await session.HandleMigrateTaskResponseAsync(gcs.SendAndResetIterationBuffer()).ConfigureAwait(false))
                        return false;
                }
                finally
                {
                    output.SpanByteAndMemory.Dispose();
                }
                return true;
            }

            /// <summary>
            /// Transmit data in namespaces during a MIGRATE ... KEYS operation.
            /// 
            /// Doesn't delete anything, just scans and transmits.
            /// </summary>
            public async ValueTask<bool> TransmitKeysNamespacesAsync(ILogger logger)
            {
                var migrateOperation = this;

                if (!await migrateOperation.InitializeAsync().ConfigureAwait(false))
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
                    migrateOperation.Scan(ref current, workerEndAddress);

                    // Stop if no keys have been found
                    if (migrateOperation.sketch.argSliceVector.IsEmpty) break;

                    logger?.LogWarning("Scan from {cursor} to {current} and discovered {count} keys", cursor, current, migrateOperation.sketch.argSliceVector.Count);

                    // Transition EPSM to MIGRATING
                    migrateOperation.sketch.SetStatus(SketchStatus.TRANSMITTING);
                    await migrateOperation.session.WaitForConfigPropagationAsync().ConfigureAwait(false);

                    // Transmit all keys gathered
                    if (!await migrateOperation.TransmitSlotsAsync().ConfigureAwait(false))
                    {
                        logger?.LogWarning("TransmitSlots failed for {cursor} to {current} (with {count} keys)", cursor, current, migrateOperation.sketch.argSliceVector.Count);
                        return false;
                    }

                    // Transition EPSM to DELETING
                    migrateOperation.sketch.SetStatus(SketchStatus.DELETING);
                    await migrateOperation.session.WaitForConfigPropagationAsync().ConfigureAwait(false);

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
                    foreach (var (ns, key, hasNs) in sketch.argSliceVector)
                    {
                        if (hasNs)
                        {
                            // Namespace'd keys are deleted as part after migration completes
                            continue;
                        }
                        else
                        {
                            _ = localServerSession.BasicGarnetApi.DELETE(key);
                        }
                    }
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

            /// <summary>
            /// Delete a Vector Set after migration if _copyOption is not set.
            /// </summary>
            public void DeleteVectorSet(PinnedSpanByte key)
            {
                if (session._copyOption)
                    return;

                var delRes = localServerSession.BasicGarnetApi.DELETE(key);

                session.logger?.LogDebug("Deleting Vector Set {key} after migration: {delRes}", System.Text.Encoding.UTF8.GetString(key), delRes);
            }
        }
    }
}