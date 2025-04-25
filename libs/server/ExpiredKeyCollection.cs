// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed partial class StoreWrapper
    {
        public async Task CollectExpiredMainStoreKeys(int collectionFrequency, long perRoundObjectCollection, CancellationToken token = default)
        {
            Debug.Assert(collectionFrequency > 0);
            try
            {
                var scratchBufferManager = new ScratchBufferManager();
                using var storageSession = new StorageSession(this, scratchBufferManager, null, null, logger);

                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    // So I take an unlocked look of the SafeReadOnlyRegion as the starting point for our scan. Now if there is any sort of shift I am not too concerned
                    // because at most one/few of my scanned records will be from a redundant region, but acquiring an epoch here would be more expensive IMO.
                    long safeInMemoryRegionAddrOfMainStore = this.store.Log.SafeReadOnlyAddress;
                    storageSession.ScanExpiredKeys(cursor: safeInMemoryRegionAddrOfMainStore, storeCursor: out long scannedTill, keys: out List<byte[]> keys, count: perRoundObjectCollection);

                    // between the scan and dels maybe a few records move to non-mutable region, but that is ok since that will just be noop in NCU section
                    RawStringInput input = new RawStringInput(RespCommand.DELIFEXPIREDINMEMORY);
                    foreach (byte[] key in keys)
                    {
                        unsafe
                        {
                            fixed (byte* keyPtr = key)
                            {
                                SpanByte keySb = SpanByte.FromPinnedPointer(keyPtr, key.Length);

                                // Use basic session for transient locking
                                storageSession.DEL_Conditional(ref keySb, ref input, ref storageSession.basicContext);
                            }
                        }

                        logger?.LogDebug("Deleted Expired Key {key}", System.Text.Encoding.UTF8.GetString(key));
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
        }

    }
}
