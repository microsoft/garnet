// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public sealed partial class AofProcessor
    {
        /// <summary>
        /// Performs recovery of the append-only file (AOF) for the specified database up to the given address,
        /// replaying records and logging recovery statistics.
        /// </summary>
        /// <param name="db">The database instance for which AOF recovery is performed.</param>
        /// <param name="untilAddress">The address up to which the AOF should be recovered.</param>
        /// <returns>The address up to which the AOF has been successfully recovered.</returns>
        public AofAddress Recover(GarnetDatabase db, AofAddress untilAddress)
        {
            Stopwatch swatch = new();
            swatch.Start();
            var total_number_of_replayed_records = 0L;
            try
            {
                storeWrapper.appendOnlyFile.CreateOrUpdateKeySequenceManager();
                logger?.LogInformation("Begin AOF recovery for DB ID: {id}", db.Id);
                return RecoverReplayDriver(db, untilAddress);
            }
            finally
            {
                storeWrapper.appendOnlyFile.ResetSequenceNumberGenerator();
                var seconds = swatch.ElapsedMilliseconds / 1000.0;
                var aofSize = db.AppendOnlyFile.TotalSize();
                var recordsPerSec = total_number_of_replayed_records / seconds;
                var GiBperSecs = aofSize / seconds / 1_000_000_000;

                logger?.LogInformation("AOF Recovery in {seconds} secs", seconds);
                logger?.LogInformation("Total number of replayed records {total_number_of_replayed_records:N0}", total_number_of_replayed_records);
                logger?.LogInformation("Throughput {recordsPerSec:N2} records/sec", recordsPerSec);
                logger?.LogInformation("AOF Recovery size {aofSize:N0}", aofSize);
                logger?.LogInformation("AOF Recovery throughput {GiBperSecs:N2} GiB/secs", GiBperSecs);
            }

            AofAddress RecoverReplayDriver(GarnetDatabase db, AofAddress untilAddress)
            {
                // Begin replay for specified database
                logger?.LogInformation("Begin AOF replay for DB ID: {id}", db.Id);
                try
                {
                    // Fetch the database AOF and update the current database context for the processor
                    var appendOnlyFile = db.AppendOnlyFile;
                    SwitchActiveDatabaseContext(db);

                    // Set the tail address for replay recovery to the tail address of the AOF if none specified
                    untilAddress.SetValueIf(appendOnlyFile.Log.TailAddress, -1);

                    var recordsReplayed = storeWrapper.serverOptions.MultiLogEnabled ? MultiLogRecover(appendOnlyFile, db.Id, untilAddress) : SingleLogRecover(appendOnlyFile, db.Id, 0, untilAddress);

                    _ = Interlocked.Add(ref total_number_of_replayed_records, recordsReplayed);
                    return untilAddress;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error occurred AofProcessor.RecoverReplay");

                    if (storeWrapper.serverOptions.FailOnRecoveryError)
                        throw;
                }

                return AofAddress.Create(storeWrapper.serverOptions.AofPhysicalSublogCount, -1);
            }
        }

        /// <summary>
        /// Replays and processes records from a specified physical sublog of the append-only file up to a given address
        /// for a particular database.
        /// </summary>
        /// <param name="appendOnlyFile">The append-only file containing the sublog to replay.</param>
        /// <param name="dbId">The identifier of the database for which the sublog is being replayed.</param>
        /// <param name="physicalSublogIdx">The index of the physical sublog to process.</param>
        /// <param name="untilAddress">The address up to which records should be replayed in the sublog.</param>
        /// <returns>A task representing the asynchronous operation, containing the number of records replayed.</returns>
        private long SingleLogRecover(GarnetAppendOnlyFile appendOnlyFile, int dbId, int physicalSublogIdx, AofAddress untilAddress)
        {
            var count = 0L;
            var beginAddress = appendOnlyFile.Log.BeginAddress;
            using var scan = appendOnlyFile.Log.Scan(physicalSublogIdx, beginAddress[physicalSublogIdx], untilAddress[physicalSublogIdx]);

            // Replay each AOF record in the current database context
            while (scan.GetNext(MemoryPool<byte>.Shared, out var entry, out var length, out _, out var nextAofAddress))
            {
                count++;
                unsafe
                {
                    fixed (byte* ptr = entry.Memory.Span)
                        ProcessAofRecordInternal(physicalSublogIdx, ptr, length, asReplica: false, out _);
                    entry.Dispose();
                }

                if (count % 100_000 == 0)
                {
                    logger?.LogTrace("Completed AOF replay of {count} records, until AOF address {nextAofAddress} (DB ID: {id})", count, nextAofAddress, dbId);
                }
            }

            logger?.LogInformation("Completed full AOF sublog {sublogIdx} replay of {count:N0} records (DB ID: {id})", physicalSublogIdx, count, dbId);
            return count;
        }

        /// <summary>
        /// Recovers log records from the specified append-only file up to a given address.
        /// </summary>
        /// <param name="appendOnlyFile">The append-only file from which log records are recovered. This file must be valid and accessible for
        /// recovery to proceed.</param>
        /// <param name="dbId">The identifier of the database for which the log recovery is being performed. This must correspond to a
        /// valid database context.</param>
        /// <param name="untilAddress">An address indicating the point up to which log records should be recovered. This must be within the valid
        /// range of the log.</param>
        /// <returns>The total number of log records that were successfully replayed during the recovery process.</returns>
        private long MultiLogRecover(GarnetAppendOnlyFile appendOnlyFile, int dbId, AofAddress untilAddress)
        {
            var recordsReplayed = 0L;
            if (appendOnlyFile.Log.RecoverLatestSequenceNumber(out var recoverUntilSequenceNumber))
            {
                var beginAddress = appendOnlyFile.Log.BeginAddress;
                var recoverDrivers = new RecoverLogDriver[untilAddress.Length];
                for (var physicalSublogIdx = 0; physicalSublogIdx < untilAddress.Length; physicalSublogIdx++)
                {
                    recoverDrivers[physicalSublogIdx] = new RecoverLogDriver(
                        this,
                        appendOnlyFile,
                        storeWrapper.serverOptions,
                        dbId,
                        physicalSublogIdx,
                        beginAddress[physicalSublogIdx],
                        untilAddress[physicalSublogIdx],
                        recoverUntilSequenceNumber,
                        logger);
                }

                try
                {
                    // TODO: Can we async this method rather than blocking?  We're in recovery.
                    var recoveryTasks = Task.WhenAll([.. recoverDrivers.Select(driver => driver.CreateRecoverTaskAsync())]);
                    AsyncUtils.BlockingWait(recoveryTasks);

                    recordsReplayed = recoverDrivers.Sum(driver => driver.ReplayedRecordCount);
                }
                finally
                {
                    for (var physicalSublogIdx = 0; physicalSublogIdx < untilAddress.Length; physicalSublogIdx++)
                        recoverDrivers[physicalSublogIdx]?.Dispose();
                }
            }

            return recordsReplayed;
        }
    }
}