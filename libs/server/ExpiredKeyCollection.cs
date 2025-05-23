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
    /*
     * Active Expiration Implementation
     * The following code is a background task that runs periodically and tombstones expired keys taking space in hybrid log memory.
     * This tombstoning increases memory re-use by free list and in chain record revivification. If you do not have revivification turned on.
     * You will not see the benefit of running the active expiration in the background.
     *
     * Records are tombstoned only from portions of the revivifiable regions of the hybrid log.
     * 
     * The most ideal garbage collection algorithm here would wake up just enough to perform effective GC rounds.
     */
    public sealed partial class StoreWrapper
    {
        public async Task AdaptiveCollectExpiredKeys(
            int percentageOfMutableLogToScan, CancellationToken token = default)
        {
            // HK TODO: Can be extended from main store to also object store by passing store as parameter here
            try
            {
                // the threshold of mutable region occupancy before 1st round of active expiration collection kicks in
                const double collectionStartingThreshold = 0.8;

                // once every 30 seconds 
                const double MINCOLLECTIONFREQUENCY_SECS = 30;
                // once every 2 hours
                const double MAXCOLLECTIONFREQ_SECS = 2 * 60 * 60;

                ulong numberOfRounds = 0;
                double sumOfYields = 0;

                // initial collection frequency is 5 minutes 
                double collectionFrequencySecs = 5 * 60;
                double lastCollectionFrequencySecs = collectionFrequencySecs;

                // any yield has to be better than last yield right
                const double STABILIZEAT_YIELD = 0.80; // if we start hitting this level of reclamation stop tuning!
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

                    (long numExpiredKeysFound, long totalRecordsScanned) = CollectExpiredMainStoreKeys(storageSession, 0, percentageOfMutableLogToScan);

                    double yieldFromRound = numExpiredKeysFound / totalRecordsScanned;
                    // yield helps us determine whether we should be increasing or decreasing the frequency here. Multiplier manages frequency changes.
                    double multiplier;
                    if (yieldFromRound == STABILIZEAT_YIELD)
                    {
                        multiplier = 1; // no change in freq
                        badYieldStreak = 0;
                        logger?.LogDebug("Colelction hit stable limit {lim} do not fine tune this round", STABILIZEAT_YIELD);
                    }
                    else if (yieldFromRound > lastYield)
                    {
                        // we did better than last time here, try to wake up a little sooner next time.
                        multiplier = rateOfAcceleration;
                        badYieldStreak = 0;
                        logger?.LogDebug("Collection hit stable limit {lim} do not fine tune this round", STABILIZEAT_YIELD);
                    }
                    else
                    {
                        // we did not collect as many as we could, try to wake up a little later next time
                        multiplier = rateOfDeceleration;
                        badYieldStreak++;
                        logger?.LogDebug("Bad Yield streak incremented: {lastYield}, {currYield}, {streak}", lastYield, yieldFromRound, badYieldStreak);
                    }

                    lastYield = yieldFromRound;

                    if (badYieldStreak < 0 && lastCollectionFrequencySecs != MAXCOLLECTIONFREQ_SECS)
                    {
                        // collectionFrequency must satisfy the bounds of min and max frequency
                        lastCollectionFrequencySecs = collectionFrequencySecs;
                        collectionFrequencySecs = Math.Min(MAXCOLLECTIONFREQ_SECS,
                            Math.Max(collectionFrequencySecs * multiplier, MINCOLLECTIONFREQUENCY_SECS));
                    }
                    else
                    {
                        // maybe we have overshot our interval and records are escaping the expirable due to speed of tail record movement
                        // in this case we slowly reduce the decay, and let the system tune itself even in periods of inactivity.
                        collectionFrequencySecs = Math.Max(MINCOLLECTIONFREQUENCY_SECS, collectionFrequencySecs * decay);
                    }

                    sumOfYields += yieldFromRound;
                    numberOfRounds++;
                    // monitoring the avgYield can tell us the eventual best expected yield for the workload with the given scan
                    // under the constraints of MIN and MAX collection frequencies.
                    double avgYield = sumOfYields / numberOfRounds;

                    logger.LogDebug("Avg Yield in {numRounds} rounds: {avg}", numberOfRounds, avgYield);

                SLEEP:
                    logger.LogDebug("Wake up and do collection again in {freq} seconds", collectionFrequencySecs);
                    await Task.Delay(TimeSpan.FromSeconds(collectionFrequencySecs), token);
                }
            }
            catch (TaskCanceledException) when (token.IsCancellationRequested) { } // Suppress the exception if the task was cancelled because of store wrapper disposal
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unknown exception received for Expired key collection task. Collection task won't be resumed.");
            }
        }

        /// <summary>
        /// Trigger actve expiration scan for a given DBId over a specified range percentage from 0-100. The range controls the aggressiveness of scanning.
        /// </summary>
        /// <param name="dbId"></param>
        /// <param name="range"></param>
        /// <returns></returns>
        public (long numExpiredKeysFound, long totalRecordsScanned) OnDemandCollectedExpiredMainStoreKeys(int dbId, int range)
        {
            var scratchBufferManager = new ScratchBufferManager();
            using var storageSession = new StorageSession(this, scratchBufferManager, null, null, logger);
            return CollectExpiredMainStoreKeys(storageSession, dbId, range);
        }

        /// <summary>
        /// scan from the back to the tail address (expired records that have not had "RMW" called on them are bound to be towards the back of the mutable region.
        /// if they had RMW called they are either having their expirations reset, or they got tombstoned via RMW.
        /// </summary>
        private (long numExpiredKeysFound, long totalRecordsScanned) CollectExpiredMainStoreKeys(StorageSession storageSession, int dbId, int range)
        {
            long scanFrom = this.store.MinRevivifiableAddress;
            long scanTill = this.store.MinRevivifiableAddress + (long)((this.store.Log.TailAddress - scanFrom) * (range / 100));

            // DELIFEXPIM will be noop for those records since they will early exit at NCU.
            (bool iteratedTillEndOfRange, long totalRecordsScanned) = storageSession.ScanExpiredKeys(cursor: scanFrom, storeCursor: out long scannedTill, keys: out List<byte[]> keys, endAddress: scanTill);

            long numExpiredKeysFound = keys.Count;

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

            return (numExpiredKeysFound, totalRecordsScanned);
        }
    }
}
