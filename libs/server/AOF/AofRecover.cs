// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public sealed unsafe partial class AofProcessor
    {
        /// <summary>
        /// Recover store using AOF
        /// </summary>
        /// <param name="db">Database to recover</param>
        /// <param name="untilAddress">Tail address for recovery</param>
        /// <returns>Tail address</returns>
        public AofAddress Recover(GarnetDatabase db, AofAddress untilAddress)
        {
            Stopwatch swatch = new();
            swatch.Start();
            var total_number_of_replayed_records = 0L;
            try
            {
                storeWrapper.appendOnlyFile.CreateOrUpdateKeySequenceManager();
                logger?.LogInformation("Begin AOF recovery for DB ID: {id}", db.Id);
                return RecoverReplay(db, untilAddress);
            }
            finally
            {
                storeWrapper.appendOnlyFile.ResetSequenceNumberGenerator();
                var seconds = swatch.ElapsedMilliseconds / 1000.0;
                var aofSize = db.AppendOnlyFile.TotalSize();
                var recordsPerSec = total_number_of_replayed_records / seconds;
                var GiBperSecs = aofSize / seconds / 1_000_000_000;

                logger?.LogInformation("AOF Recovery in {seconds} secs", seconds);
                logger?.LogInformation("Total number of replayed records {total_number_of_replayed_records:N0} bytes", total_number_of_replayed_records);
                logger?.LogInformation("Throughput {recordsPerSec:N2} records/sec", recordsPerSec);
                logger?.LogInformation("AOF Recovery size {aofSize:N0}", aofSize);
                logger?.LogInformation("AOF Recovery throughput {GiBperSecs:N2} GiB/secs", GiBperSecs);
            }

            AofAddress RecoverReplay(GarnetDatabase db, AofAddress untilAddress)
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

                    var tasks = new Task[untilAddress.Length];
                    for (var i = 0; i < untilAddress.Length; i++)
                    {
                        var sublogIdx = i;
                        tasks[i] = Task.Run(() => RecoverReplayPhysicalSublog(sublogIdx, untilAddress));
                    }

                    Task.WaitAll(tasks);

                    void RecoverReplayPhysicalSublog(int physicalSublogIdx, AofAddress untilAddress)
                    {
                        var count = 0;
                        var beginAddress = appendOnlyFile.Log.BeginAddress;
                        using var scan = appendOnlyFile.Scan(physicalSublogIdx, beginAddress[physicalSublogIdx], untilAddress[physicalSublogIdx]);

                        // Replay each AOF record in the current database context
                        while (scan.GetNext(MemoryPool<byte>.Shared, out var entry, out var length, out _, out var nextAofAddress))
                        {
                            count++;
                            fixed (byte* ptr = entry.Memory.Span)
                                ProcessAofRecordInternal(physicalSublogIdx, ptr, length, asReplica: false, out _);
                            entry.Dispose();

                            if (count % 100_000 == 0)
                                logger?.LogTrace("Completed AOF replay of {count} records, until AOF address {nextAofAddress} (DB ID: {id})", count, nextAofAddress, db.Id);
                        }

                        logger?.LogInformation("Completed full AOF sublog {sublogIdx} replay of {count:N0} records (DB ID: {id})", physicalSublogIdx, count, db.Id);
                        _ = Interlocked.Add(ref total_number_of_replayed_records, count);
                    }

                    return untilAddress;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error occurred AofProcessor.RecoverReplay");

                    if (storeWrapper.serverOptions.FailOnRecoveryError)
                        throw;
                }
                finally
                {
                    aofReplayCoordinator.Dispose();
                    foreach (var respServerSession in respServerSessions)
                        respServerSession.Dispose();
                }

                return AofAddress.Create(storeWrapper.serverOptions.AofPhysicalSublogCount, -1);
            }
        }
    }
}
