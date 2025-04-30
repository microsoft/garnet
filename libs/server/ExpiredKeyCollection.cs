// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed partial class StoreWrapper
    {
        public async Task CollectExpiredMainStoreKeys(int collectionFrequency, long perRoundMaxRecordsToCollect, CancellationToken token = default)
        {
            try
            {
                var scratchBufferManager = new ScratchBufferManager();
                using var storageSession = new StorageSession(this, scratchBufferManager, null, null, logger);
                while (true)
                {
                    if (token.IsCancellationRequested) return;
                    // Take an unprotected look from the SafeReadOnlyRegion as the starting point for our scan.
                    // If there is any sort of shift of the marker then a few of my scanned records will be from a redundant region.
                    // DELIFEXPIREDINMEMORY will be noop for those records since they will early exit at NCU.
                    long safeInMemoryRegionAddrOfMainStore = this.store.Log.SafeReadOnlyAddress;
                    storageSession.ScanExpiredKeys(cursor: safeInMemoryRegionAddrOfMainStore, storeCursor: out long scannedTill, keys: out List<byte[]> keys, count: perRoundMaxRecordsToCollect);
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
            catch (TaskCanceledException) when (token.IsCancellationRequested) { } // Suppress the exception if the task was cancelled because of store wrapper disposal
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unknown exception received for Expired key collection task. Collection task won't be resumed.");
            }
        }
    }
}
