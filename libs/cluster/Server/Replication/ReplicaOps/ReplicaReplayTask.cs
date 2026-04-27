// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IBulkLogEntryConsumer, IDisposable
    {
        TsavoriteLogScanSingleIterator replayIterator = null;
        CancellationTokenSource replicaReplayTaskCts;
        SingleWriterMultiReaderLock activeReplay;

        /// <summary>
        /// Reset background replay iterator
        /// </summary>
        public void ResetReplayIterator()
        {
            ResetReplayCts();
            replayIterator?.Dispose();
            replayIterator = null;

            void ResetReplayCts()
            {
                if (replicaReplayTaskCts == null)
                {
                    replicaReplayTaskCts = CancellationTokenSource.CreateLinkedTokenSource(ctsRepManager.Token);
                }
                else
                {
                    replicaReplayTaskCts.Cancel();
                    try
                    {
                        activeReplay.WriteLock();
                        replicaReplayTaskCts.Dispose();
                        replicaReplayTaskCts = CancellationTokenSource.CreateLinkedTokenSource(ctsRepManager.Token);
                    }
                    finally
                    {
                        activeReplay.WriteUnlock();
                    }
                }
            }
        }

        public void Throttle() { }

        public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            ReplicationOffset = currentAddress;

            var offsetUpdate = 0L;

            try
            {
                var ptr = record;
                while (ptr < record + recordLength)
                {
                    replicaReplayTaskCts.Token.ThrowIfCancellationRequested();
                    var entryLength = storeWrapper.appendOnlyFile.HeaderSize;
                    var payloadLength = storeWrapper.appendOnlyFile.UnsafeGetLength(ptr);
                    if (payloadLength > 0)
                    {
                        aofProcessor.ProcessAofRecordInternal(ptr + entryLength, payloadLength, true, out var isCheckpointStart);
                        // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                        // point when we take a checkpoint at the checkpoint end marker
                        if (isCheckpointStart)
                            ReplicationCheckpointStartOffset = ReplicationOffset + offsetUpdate;
                        entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                    }
                    else if (payloadLength < 0)
                    {
                        if (!clusterProvider.serverOptions.EnableFastCommit)
                        {
                            throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);
                        }
                        TsavoriteLogRecoveryInfo info = new();
                        info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                        storeWrapper.appendOnlyFile?.UnsafeCommitMetadataOnly(info, isProtected);
                        entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                    }
                    ptr += entryLength;

                    offsetUpdate += entryLength;
                }
            }
            catch
            {
                // If an exception occurrs, be sure to advance ReplicationOffset by the amount of successful work that transpired before the error

                ReplicationOffset += offsetUpdate;
                throw;
            }

            // Before updating replication offset, we must wait for any pending Vector Set ops to complete
            aofProcessor.WaitForVectorOperationsToComplete();

            // Do the final offset update - we defer until here so Vector Set operations can proceed without waiting after each record is applied
            ReplicationOffset += offsetUpdate;

            if (ReplicationOffset != nextAddress)
            {
                logger?.LogError("ReplicaReplayTask.Consume NextAddress Mismatch recordLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{ReplicationOffset}", recordLength, currentAddress, nextAddress, ReplicationOffset);
                throw new GarnetException($"ReplicaReplayTask.Consume NextAddress Mismatch recordeLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{ReplicationOffset}", LogLevel.Warning, clientResponse: false);
            }
        }

        public async Task ReplicaReplayTaskAsync()
        {
            try
            {
                activeReplay.ReadLock();
                while (true)
                {
                    replicaReplayTaskCts.Token.ThrowIfCancellationRequested();
                    await replayIterator.BulkConsumeAllAsync(
                        this,
                        clusterProvider.serverOptions.ReplicaSyncDelayMs,
                        maxChunkSize: 1 << 20,
                        replicaReplayTaskCts.Token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaReplayTask - terminating");
            }
            finally
            {
                activeReplay.ReadUnlock();
            }
        }
    }
}