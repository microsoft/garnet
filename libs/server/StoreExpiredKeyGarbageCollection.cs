// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public sealed partial class  StoreWrapper
    {
        public async Task CollectExpiredKeys(int collectionFrequency, CancellationToken token = default)
        {
            Debug.Assert(collectionFrequency > 0);
            try
            {
                var scratchBufferManager = new ScratchBufferManager();
                using var storageSession = new StorageSession(this, scratchBufferManager, null, null, logger);

                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    // perRoundObjectCollection can be adaptive based on memory pressure of some sort?

                    // First scan and collect all keys in In-Memory
                    storageSession.DbScan(patternB: allPattern, cursor: StartOfMainMemoryAddress, keys: out List<byte[]> keys, count: perRoundObjectCollection, typeObject: typeObject);

                    foreach (byte[] key in keys)
                    {
                        // DELIFEXPIREDINMEMORY each key
                    }

                    await Task.Delay(TimeSpan.FromSeconds(collectionFrequency), token);
                }
            }
            catch (TaskCanceledException) when (token.IsCancellationRequested)
            {
                // Suppress the exception if the task was cancelled because of store wrapper disposal
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unknown exception received for Expired key collection task. Collection task won't be resumed.");
            }

            /*
                var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HCOLLECT };
                var input = new ObjectInput(header);

                ReadOnlySpan<ArgSlice> key = [ArgSlice.FromPinnedSpan("*"u8)];
                storageSession.HashCollect(key, ref input, ref storageSession.objectStoreBasicContext);
                scratchBufferManager.Reset();
            */
        }

    }
}
