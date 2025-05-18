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
    /*
     * Active Expiration Implementation
     * The following code is a background task that runs periodically and tombstones expired keys taking space in hybrid log memory.
     * This tombstoning increases memory re-use by free list and in chain record revivification. If you do not have revivification turned on.
     * You will not see the benefit of running the active expiration in the background.
     *
     * Records are tombstoned only from portions of the revivifiable regions of the hybrid log.
     * 
     * The most ideal garbage collection algorithm here would wake up just enough to perform effective GC rounds.
     * 
     */
    public sealed partial class StoreWrapper
    {
        private long lastScannedTill = -1;

        // Can be extended for main store vs object store by passing store as parameter here
        public async Task CollectExpiredMainStoreKeys(double percentageOfMutableLogToScan, CancellationToken token = default)
        {
            try
            {
                // the threshold of mutable region occupancy before 1st round of active expiration collection kicks in
                const double collectionStartingThreshold = 0.8;

                const double MINCOLLECTIONFREQUENCY_SECS = 60;
                const double MAXCOLLECTIONFREQ_SECS = 60 * 60;

                // initial collection frequency is 5 minutes 
                double collectionFrequencySecs = 5 * 60;
                double lastCollectionFrequencySecs = collectionFrequencySecs;

                // any yield has to be better than last yield right
                const double minGoodYield = 0.25;
                // can I use lastYield here instead? to do this dynamically???

                double lastYield = -1;
                double badYieldStreak = 0;

                // Use exponential increase and decrease of frequency based on yield
                // rate of accl decides how quickly we increase frequency of collection
                const double rateOfAcceleration = 0.5;
                // rate of decel decides how quickly we decrease frequency of colleciton
                const double rateOfDeceleration = 1.5;

                // decay rate is a voodoo number to help us from not stalling at maxFrequency forever
                const double decay = 0.85;

                var scratchBufferManager = new ScratchBufferManager();
                using var storageSession = new StorageSession(this, scratchBufferManager, null, null, logger);

                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    // don't even attempt reclamation at below threshold of mutable region occupation
                    double currentMutabelRegionUsagePercentage = (store.Log.TailAddress - store.Log.ReadOnlyAddress) / store.MaxSizeOfMutableRegionInBytes;

                    if (currentMutabelRegionUsagePercentage < collectionStartingThreshold)
                    {
                        logger?.LogDebug("Current hybrid log mutable region percentage {usage} is below threshold {thresh}. Skipping collection.",
                            currentMutabelRegionUsagePercentage, collectionStartingThreshold);
                        goto SLEEP;
                    }

                    // scan from the back to the tail address (expired records that have not had "RMW" called on them are bound to be towards the back of the mutable region.
                    // if they had RMW called they are either having their expirations reset, or they got tombstoned via RMW.
                    // This assumes that ttl's don't follow a normal distribution. Since some of the most dominant usecase of Garnet does batched writes via Txns and set the same expirations.
                    // If we wanted to scan the log with TTL following normal distribution we could do less frequent full scans to find mean and stddev of TTLS, then 
                    long scanFrom = this.store.MinRevivifiableAddress;
                    long scanTill = this.store.MinRevivifiableAddress + (long)((this.store.Log.TailAddress - scanFrom) * percentageOfMutableLogToScan);

                    // DELIFEXPIM will be noop for those records since they will early exit at NCU.
                    (bool iteratedTillEndOfRange, long totalRecordsScanned) = storageSession.ScanExpiredKeys(cursor: scanFrom, storeCursor: out long scannedTill, keys: out List<byte[]> keys, endAddress: scanTill);

                    int numExpiredKeysFound = keys.Count;

                    Debug.Assert(totalRecordsScanned > 0, "Since we don't trigger collection till a threshold, in no scenario should we get a 0 value for scanning." +
                        "The value being greater than zero is an invariant. Violation will cause issues in yield calculation.");

                    RawStringInput input = new RawStringInput(RespCommand.DELIFEXPIM);
                    // If there is any sort of shift of the marker then a few of my scanned records will be from a redundant region.
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

                    logger?.LogDebug("Deleted {numKeys} keys out {totalRecords} records in range {start} to {end}", numExpiredKeysFound, totalRecordsScanned, scanFrom, scannedTill);

                    double yield = numExpiredKeysFound / totalRecordsScanned;
                    // yield helps us determine whether we should be increasing or decreasing the frequency here. Multiplier manages frequency changes.
                    double multiplier;
                    if (yield > minGoodYield)
                    {
                        // we did better than min acceptable here
                        // now there are two possibilities.
                        multiplier = rateOfAcceleration;
                        lastYield = yield;
                        badYieldStreak = 0;
                    }
                    else
                    {
                        multiplier = rateOfDeceleration;
                        lastYield = yield;
                        badYieldStreak++;
                    }

                    if (badYieldStreak < 0 && lastCollectionFrequencySecs != MAXCOLLECTIONFREQ_SECS)
                    {
                        // collectionFrequency must satisfy the bounds of min and max frequency
                        lastCollectionFrequencySecs = collectionFrequencySecs;
                        collectionFrequencySecs = Math.Min(MAXCOLLECTIONFREQ_SECS,
                            Math.Max(collectionFrequencySecs * multiplier, MINCOLLECTIONFREQUENCY_SECS));
                    }
                    else
                    {
                        // maybe we have severly overshot our interval and records are escaping the expirable range
                        // due to speed of tail record movement
                        collectionFrequencySecs *= decay;
                    }

                SLEEP:
                    await Task.Delay(TimeSpan.FromSeconds(collectionFrequencySecs), token);
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
