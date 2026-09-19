// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Streams new AOF records from a standalone primary over an established PSYNC connection.
    /// </summary>
    internal sealed class StandaloneSyncDriver : IDisposable
    {
        const int MaxBatchBytes = 256 * 1024;
        const int MaxBatchRecords = 256;

        readonly StoreWrapper storeWrapper;
        readonly INetworkSender networkSender;
        readonly ILogger logger;
        readonly CancellationTokenSource cts = new();
        readonly Task syncTask;
        readonly AofRetentionLease retentionLease;
        AofRetentionLease snapshotRetentionLease;
        CheckpointLease<CheckpointMetadata> checkpointLease;
        TsavoriteCheckpointDataSourceReader checkpointReader;
        long retainedAddress;
        TsavoriteLogScanSingleIterator iterator;

        public StandaloneSyncDriver(
            StoreWrapper storeWrapper,
            INetworkSender networkSender,
            long startAddress,
            ILogger logger,
            CheckpointLease<CheckpointMetadata> checkpointLease = null,
            AofRetentionLease snapshotRetentionLease = null,
            LogFileInfo logFileInfo = default,
            long indexSize = 0)
        {
            this.storeWrapper = storeWrapper;
            this.networkSender = networkSender;
            this.logger = logger;
            this.checkpointLease = checkpointLease;
            this.snapshotRetentionLease = snapshotRetentionLease;
            retainedAddress = startAddress;

            if (checkpointLease != null)
            {
                checkpointReader = new TsavoriteCheckpointDataSourceReader(
                    new CheckpointFileTransferProvider(storeWrapper.serverOptions, storeWrapper.StoreCheckpointManager),
                    checkpointLease.Value,
                    logFileInfo,
                    indexSize,
                    storeWrapper.serverOptions.ReplicaSyncTimeout,
                    logger);
            }

            var retentionStartAddress = AofAddress.Create(1, startAddress);
            if (!storeWrapper.appendOnlyFile.RetentionManager.TryAcquire(
                retentionStartAddress,
                _ => Volatile.Read(ref retainedAddress),
                allowDataLoss: false,
                out retentionLease))
            {
                throw new GarnetException($"Cannot stream AOF from {startAddress}; data has already been truncated");
            }

            syncTask = Task.Run(() => StreamAsync(startAddress, cts.Token));
        }

        async Task StreamAsync(long startAddress, CancellationToken token)
        {
            try
            {
                if (checkpointReader != null)
                {
                    try
                    {
                        await SendCheckpointAsync(token).ConfigureAwait(false);
                    }
                    finally
                    {
                        ReleaseSnapshotResources();
                    }
                }
                else
                {
                    SendFrame(
                        StandaloneReplicationFrameType.StreamReady,
                        CheckpointFileType.NONE,
                        default,
                        startAddress,
                        []);
                }

                iterator = storeWrapper.appendOnlyFile.Log.ScanSingle(
                    0,
                    startAddress,
                    long.MaxValue,
                    scanUncommitted: true,
                    recover: false,
                    logger: logger);

                var batch = new ArrayBufferWriter<byte>(MaxBatchBytes);
                while (!token.IsCancellationRequested)
                {
                    var recordCount = 0;
                    var batchStartAddress = 0L;
                    var batchEndAddress = iterator.NextAddress;

                    while (recordCount < MaxBatchRecords &&
                           batch.WrittenCount < MaxBatchBytes &&
                           iterator.GetNext(out var entry, out var entryLength, out var currentAddress, out var nextAddress))
                    {
                        if (recordCount == 0)
                            batchStartAddress = currentAddress;
                        StandaloneReplicationWireFormat.WriteAofBatchRecord(
                            batch,
                            currentAddress,
                            entry.AsSpan(0, entryLength));
                        batchEndAddress = nextAddress;
                        recordCount++;
                    }

                    if (recordCount > 0)
                    {
                        SendFrame(
                            StandaloneReplicationFrameType.AofBatch,
                            CheckpointFileType.NONE,
                            default,
                            batchStartAddress,
                            batch.WrittenSpan);
                        Volatile.Write(ref retainedAddress, batchEndAddress);
                        batch.Clear();
                        networkSender.Throttle();
                        continue;
                    }

                    if (!await iterator.WaitAsync(token).ConfigureAwait(false))
                        break;
                }

                async Task SendCheckpointAsync(CancellationToken token)
                {
                    foreach (var dataSource in checkpointReader.GetDataSources())
                    {
                        try
                        {
                            while (dataSource.HasNextChunk)
                            {
                                var result = await dataSource.ReadNextChunkAsync(token).ConfigureAwait(false);
                                if (result.Buffer != null)
                                {
                                    try
                                    {
                                        unsafe
                                        {
                                            SendFrame(
                                                StandaloneReplicationFrameType.CheckpointFile,
                                                dataSource.Type,
                                                dataSource.Token,
                                                result.ChunkStartAddress,
                                                new ReadOnlySpan<byte>(result.Buffer.aligned_pointer, result.BytesRead));
                                        }
                                    }
                                    finally
                                    {
                                        result.Buffer.Return();
                                    }
                                }
                                else
                                {
                                    SendFrame(
                                        StandaloneReplicationFrameType.CheckpointMetadata,
                                        dataSource.Type,
                                        dataSource.Token,
                                        -1,
                                        result.Data);
                                }
                                networkSender.Throttle();
                            }

                            if (dataSource is not TsavoriteMetadataSource)
                            {
                                SendFrame(
                                    StandaloneReplicationFrameType.CheckpointFileEnd,
                                    dataSource.Type,
                                    dataSource.Token,
                                    dataSource.CurrentOffset,
                                    []);
                            }
                        }
                        finally
                        {
                            dataSource.Dispose();
                        }
                    }

                    var metadata = checkpointLease.Value;
                    SendFrame(
                        StandaloneReplicationFrameType.CheckpointComplete,
                        CheckpointFileType.NONE,
                        metadata.storeHlogToken,
                        metadata.storeCheckpointCoveredAofAddress[0],
                        StandaloneReplicationWireFormat.SerializeCheckpointMetadata(metadata));
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (ObjectDisposedException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Standalone AOF stream failed");
                networkSender.TryClose();
            }
            finally
            {
                iterator?.Dispose();
                iterator = null;
            }
        }

        void ReleaseSnapshotResources()
        {
            Interlocked.Exchange(ref checkpointReader, null)?.Dispose();
            Interlocked.Exchange(ref checkpointLease, null)?.Dispose();
            Interlocked.Exchange(ref snapshotRetentionLease, null)?.Dispose();
        }

        unsafe void SendFrame(
            StandaloneReplicationFrameType type,
            CheckpointFileType checkpointFileType,
            Guid token,
            long address,
            ReadOnlySpan<byte> payload)
        {
            Span<byte> header = stackalloc byte[StandaloneReplicationWireFormat.HeaderLength];
            StandaloneReplicationWireFormat.WriteHeader(header, type, checkpointFileType, payload.Length, token, address);

            networkSender.EnterAndGetResponseObject(out var head, out var tail);
            try
            {
                Write(header, ref head, tail);
                Write(payload, ref head, tail);
                Flush(ref head);
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
            }

            void Write(ReadOnlySpan<byte> source, ref byte* destination, byte* end)
            {
                while (!source.IsEmpty)
                {
                    var available = (int)(end - destination);
                    if (available == 0)
                    {
                        Flush(ref destination);
                        networkSender.GetResponseObject();
                        destination = networkSender.GetResponseObjectHead();
                        end = networkSender.GetResponseObjectTail();
                        available = (int)(end - destination);
                    }

                    var count = Math.Min(source.Length, available);
                    source[..count].CopyTo(new Span<byte>(destination, count));
                    destination += count;
                    source = source[count..];
                }
            }

            void Flush(ref byte* destination)
            {
                var responseHead = networkSender.GetResponseObjectHead();
                var length = (int)(destination - responseHead);
                if (length > 0)
                {
                    networkSender.SendResponse(0, length);
                    destination = null;
                }
            }
        }

        public void Dispose()
        {
            cts.Cancel();
            iterator?.Dispose();
            ReleaseSnapshotResources();
            retentionLease.Dispose();
            _ = syncTask.ContinueWith(
                static (_, state) => ((CancellationTokenSource)state).Dispose(),
                cts,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }
}