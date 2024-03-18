// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Log commit manager for a generic IDevice
    /// </summary>
    public class DeviceLogCommitCheckpointManager : ILogCommitManager, ICheckpointManager
    {
        const byte indexTokenCount = 2;
        const byte logTokenCount = 1;
        const byte flogCommitCount = 1;

        /// <summary>
        /// deviceFactory
        /// </summary>
        protected readonly INamedDeviceFactory deviceFactory;

        /// <summary>
        /// checkpointNamingScheme
        /// </summary>
        protected readonly ICheckpointNamingScheme checkpointNamingScheme;
        private readonly SemaphoreSlim semaphore;

        private readonly bool removeOutdated;
        private SectorAlignedBufferPool bufferPool;

        /// <summary>
        /// Track historical commits for automatic purging
        /// </summary>
        private readonly Guid[] indexTokenHistory, logTokenHistory;
        private readonly long[] flogCommitHistory;
        private byte indexTokenHistoryOffset, logTokenHistoryOffset, flogCommitHistoryOffset;

        readonly ILogger logger;
        readonly WorkQueueFIFO<long> deleteQueue;
        readonly int fastCommitThrottleFreq;
        int commitCount;

        /// <summary>
        /// Create new instance of log commit manager
        /// </summary>
        /// <param name="deviceFactory">Factory for getting devices</param>
        /// <param name="checkpointNamingScheme">Checkpoint naming helper</param>
        /// <param name="removeOutdated">Remote older Tsavorite log commits</param>
        /// <param name="fastCommitThrottleFreq">FastCommit throttle frequency - use only in FastCommit mode</param>
        /// <param name="logger">Remote older Tsavorite log commits</param>
        public DeviceLogCommitCheckpointManager(INamedDeviceFactory deviceFactory, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = true, int fastCommitThrottleFreq = 0, ILogger logger = null)
        {
            this.logger = logger;
            this.deviceFactory = deviceFactory;
            this.checkpointNamingScheme = checkpointNamingScheme;
            this.fastCommitThrottleFreq = fastCommitThrottleFreq;

            semaphore = new SemaphoreSlim(0);

            this.removeOutdated = removeOutdated;
            if (removeOutdated)
            {
                deleteQueue = new WorkQueueFIFO<long>(prior => deviceFactory.Delete(checkpointNamingScheme.TsavoriteLogCommitMetadata(prior)));

                // We keep two index checkpoints as the latest index might not have a
                // later log checkpoint to work with
                indexTokenHistory = new Guid[indexTokenCount];
                // We only keep the latest log checkpoint
                logTokenHistory = new Guid[logTokenCount];
                // // We only keep the latest TsavoriteLog commit
                flogCommitHistory = new long[flogCommitCount];
            }
            deviceFactory.Initialize(checkpointNamingScheme.BaseName());
        }

        /// <inheritdoc />
        public void PurgeAll()
        {
            deviceFactory.Delete(new FileDescriptor { directoryName = "" });
        }

        /// <inheritdoc />
        public void Purge(Guid token)
        {
            // Try both because we do not know which type the guid denotes
            deviceFactory.Delete(checkpointNamingScheme.LogCheckpointBase(token));
            deviceFactory.Delete(checkpointNamingScheme.IndexCheckpointBase(token));
        }

        /// <summary>
        /// Create new instance of log commit manager
        /// </summary>
        /// <param name="deviceFactory">Factory for getting devices</param>
        /// <param name="baseName">Overall location specifier (e.g., local path or cloud container name)</param>
        /// <param name="removeOutdated">Remote older Tsavorite log commits</param>
        /// <param name="logger">Remote older Tsavorite log commits</param>
        public DeviceLogCommitCheckpointManager(INamedDeviceFactory deviceFactory, string baseName, bool removeOutdated = false, ILogger logger = null)
            : this(deviceFactory, new DefaultCheckpointNamingScheme(baseName), removeOutdated)
        {
            this.logger = logger;
        }

        #region ILogCommitManager

        /// <inheritdoc />
        public unsafe void Commit(long beginAddress, long untilAddress, byte[] commitMetadata, long commitNum, bool forceWriteMetadata)
        {
            if (!forceWriteMetadata && fastCommitThrottleFreq > 0 && (commitCount++ % fastCommitThrottleFreq != 0)) return;

            using var device = deviceFactory.Get(checkpointNamingScheme.TsavoriteLogCommitMetadata(commitNum));

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);
            writer.Write(commitMetadata.Length);
            writer.Write(commitMetadata);

            WriteInto(device, 0, ms.ToArray(), (int)ms.Position);

            if (removeOutdated)
            {
                var prior = flogCommitHistory[flogCommitHistoryOffset];
                flogCommitHistory[flogCommitHistoryOffset] = commitNum;
                flogCommitHistoryOffset = (byte)((flogCommitHistoryOffset + 1) % flogCommitCount);
                if (prior != default)
                {
                    // System.Threading.Tasks.Task.Run(() => deviceFactory.Delete(checkpointNamingScheme.TsavoriteLogCommitMetadata(prior)));
                    deleteQueue.EnqueueAndTryWork(prior, true);
                }
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public IEnumerable<long> ListCommits()
        {
            return deviceFactory.ListContents(checkpointNamingScheme.TsavoriteLogCommitBasePath()).Select(checkpointNamingScheme.CommitNumber).OrderByDescending(e => e);
        }

        /// <inheritdoc />
        public void RemoveCommit(long commitNum)
        {
            deviceFactory.Delete(checkpointNamingScheme.TsavoriteLogCommitMetadata(commitNum));
        }

        /// <inheritdoc />
        public void RemoveAllCommits()
        {
            foreach (var commitNum in ListCommits())
                RemoveCommit(commitNum);
        }

        /// <inheritdoc />
        public byte[] GetCommitMetadata(long commitNum)
        {
            using var device = deviceFactory.Get(checkpointNamingScheme.TsavoriteLogCommitMetadata(commitNum));

            ReadInto(device, 0, out byte[] writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));

            return new Span<byte>(body).Slice(sizeof(int)).ToArray();
        }
        #endregion

        #region ICheckpointManager

        /// <inheritdoc />
        public unsafe void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            var device = NextIndexCheckpointDevice(indexToken);

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);
            writer.Write(commitMetadata.Length);
            writer.Write(commitMetadata);

            WriteInto(device, 0, ms.ToArray(), (int)ms.Position);
            device.Dispose();

            if (removeOutdated)
            {
                var prior = indexTokenHistory[indexTokenHistoryOffset];
                indexTokenHistory[indexTokenHistoryOffset] = indexToken;
                indexTokenHistoryOffset = (byte)((indexTokenHistoryOffset + 1) % indexTokenCount);
                if (prior != default)
                    deviceFactory.Delete(checkpointNamingScheme.IndexCheckpointBase(prior));
            }
        }

        /// <inheritdoc />
        public IEnumerable<Guid> GetIndexCheckpointTokens()
        {
            return deviceFactory.ListContents(checkpointNamingScheme.IndexCheckpointBasePath()).Select(checkpointNamingScheme.Token);
        }

        /// <inheritdoc />
        public byte[] GetIndexCheckpointMetadata(Guid indexToken)
        {
            var device = deviceFactory.Get(checkpointNamingScheme.IndexCheckpointMetadata(indexToken));

            ReadInto(device, 0, out byte[] writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));
            device.Dispose();
            return new Span<byte>(body).Slice(sizeof(int)).ToArray();
        }

        /// <inheritdoc />
        public virtual unsafe void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            var device = NextLogCheckpointDevice(logToken);

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);
            writer.Write(commitMetadata.Length);
            writer.Write(commitMetadata);

            WriteInto(device, 0, ms.ToArray(), (int)ms.Position);
            device.Dispose();

            if (removeOutdated)
            {
                var prior = logTokenHistory[logTokenHistoryOffset];
                logTokenHistory[logTokenHistoryOffset] = logToken;
                logTokenHistoryOffset = (byte)((logTokenHistoryOffset + 1) % logTokenCount);
                if (prior != default)
                    deviceFactory.Delete(checkpointNamingScheme.LogCheckpointBase(prior));
            }
        }

        /// <inheritdoc />
        public virtual unsafe void CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata, DeltaLog deltaLog)
        {
            deltaLog.Allocate(out int length, out long physicalAddress);
            if (length < commitMetadata.Length)
            {
                deltaLog.Seal(0, DeltaLogEntryType.CHECKPOINT_METADATA);
                deltaLog.Allocate(out length, out physicalAddress);
                if (length < commitMetadata.Length)
                {
                    deltaLog.Seal(0);
                    throw new Exception($"Metadata of size {commitMetadata.Length} does not fit in delta log space of size {length}");
                }
            }
            fixed (byte* ptr = commitMetadata)
            {
                Buffer.MemoryCopy(ptr, (void*)physicalAddress, commitMetadata.Length, commitMetadata.Length);
            }
            deltaLog.Seal(commitMetadata.Length, DeltaLogEntryType.CHECKPOINT_METADATA);
            deltaLog.FlushAsync().Wait();
        }

        /// <inheritdoc />
        public IEnumerable<Guid> GetLogCheckpointTokens()
        {
            return deviceFactory.ListContents(checkpointNamingScheme.LogCheckpointBasePath()).Select(checkpointNamingScheme.Token);
        }

        /// <inheritdoc />
        public virtual byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
        {
            byte[] metadata = null;
            if (deltaLog != null && scanDelta)
            {
                // Try to get latest valid metadata from delta-log
                deltaLog.Reset();
                while (deltaLog.GetNext(out long physicalAddress, out int entryLength, out var type))
                {
                    switch (type)
                    {
                        case DeltaLogEntryType.DELTA:
                            // consider only metadata records
                            continue;
                        case DeltaLogEntryType.CHECKPOINT_METADATA:
                            metadata = new byte[entryLength];
                            unsafe
                            {
                                fixed (byte* m = metadata)
                                    Buffer.MemoryCopy((void*)physicalAddress, m, entryLength, entryLength);
                            }
                            HybridLogRecoveryInfo recoveryInfo = new();
                            using (StreamReader s = new(new MemoryStream(metadata)))
                            {
                                recoveryInfo.Initialize(s);
                                // Finish recovery if only specific versions are requested
                                if (recoveryInfo.version == recoverTo || recoveryInfo.version < recoverTo && recoveryInfo.nextVersion > recoverTo) goto LoopEnd;
                            }
                            continue;
                        default:
                            throw new TsavoriteException("Unexpected entry type");
                    }
                LoopEnd:
                    break;
                }
                if (metadata != null) return metadata;

            }

            var device = deviceFactory.Get(checkpointNamingScheme.LogCheckpointMetadata(logToken));

            ReadInto(device, 0, out byte[] writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));
            device.Dispose();
            return body.AsSpan().Slice(sizeof(int), size).ToArray();
        }

        /// <inheritdoc />
        public IDevice GetIndexDevice(Guid indexToken)
        {
            return deviceFactory.Get(checkpointNamingScheme.HashTable(indexToken));
        }

        /// <inheritdoc />
        public IDevice GetSnapshotLogDevice(Guid token)
        {
            return deviceFactory.Get(checkpointNamingScheme.LogSnapshot(token));
        }

        /// <inheritdoc />
        public IDevice GetSnapshotObjectLogDevice(Guid token)
        {
            return deviceFactory.Get(checkpointNamingScheme.ObjectLogSnapshot(token));
        }

        /// <inheritdoc />
        public IDevice GetDeltaLogDevice(Guid token)
        {
            return deviceFactory.Get(checkpointNamingScheme.DeltaLog(token));
        }

        /// <inheritdoc />
        public void InitializeIndexCheckpoint(Guid indexToken)
        {
        }

        /// <inheritdoc />
        public void InitializeLogCheckpoint(Guid logToken)
        {
        }

        /// <inheritdoc />
        public void OnRecovery(Guid indexToken, Guid logToken)
        {
            if (!removeOutdated) return;

            // Add recovered tokens to history, for eventual purging
            if (indexToken != default)
            {
                indexTokenHistory[indexTokenHistoryOffset] = indexToken;
                indexTokenHistoryOffset = (byte)((indexTokenHistoryOffset + 1) % indexTokenCount);
            }
            if (logToken != default)
            {
                logTokenHistory[logTokenHistoryOffset] = logToken;
                logTokenHistoryOffset = (byte)((logTokenHistoryOffset + 1) % logTokenCount);
            }

            // Purge all log checkpoints that were not used for recovery
            foreach (var recoveredLogToken in GetLogCheckpointTokens())
            {
                if (recoveredLogToken != logToken)
                    deviceFactory.Delete(checkpointNamingScheme.LogCheckpointBase(recoveredLogToken));
            }

            // Purge all index checkpoints that were not used for recovery
            foreach (var recoveredIndexToken in GetIndexCheckpointTokens())
            {
                if (recoveredIndexToken != indexToken)
                    deviceFactory.Delete(checkpointNamingScheme.IndexCheckpointBase(recoveredIndexToken));
            }
        }

        /// <inheritdoc />
        public void OnRecovery(long commitNum)
        {
            if (!removeOutdated) return;

            foreach (var recoveredCommitNum in ListCommits())
                if (recoveredCommitNum != commitNum) RemoveCommit(recoveredCommitNum);

            // Add recovered tokens to history, for eventual purging
            if (commitNum != default)
            {
                flogCommitHistory[flogCommitHistoryOffset] = commitNum;
                flogCommitHistoryOffset = (byte)((flogCommitHistoryOffset + 1) % flogCommitCount);
            }
        }

        private IDevice NextIndexCheckpointDevice(Guid token)
            => deviceFactory.Get(checkpointNamingScheme.IndexCheckpointMetadata(token));

        private IDevice NextLogCheckpointDevice(Guid token)
            => deviceFactory.Get(checkpointNamingScheme.LogCheckpointMetadata(token));
        #endregion

        private unsafe void IOCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                logger?.LogError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }
            semaphore.Release();
        }

        /// <summary>
        /// Note: will read potentially more data (based on sector alignment)
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        protected unsafe void ReadInto(IDevice device, ulong address, out byte[] buffer, int size)
        {
            if (bufferPool == null)
                bufferPool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToRead);
            device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer,
                (uint)numBytesToRead, IOCallback, null);
            semaphore.Wait();

            buffer = new byte[numBytesToRead];
            fixed (byte* bufferRaw = buffer)
                Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, numBytesToRead, numBytesToRead);
            pbuffer.Return();
        }

        /// <summary>
        /// Note: pads the bytes with zeros to achieve sector alignment
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        protected unsafe void WriteInto(IDevice device, ulong address, byte[] buffer, int size)
        {
            if (bufferPool == null)
                bufferPool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToWrite = size;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToWrite);
            fixed (byte* bufferRaw = buffer)
            {
                Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, size, size);
            }

            device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, (uint)numBytesToWrite, IOCallback, null);
            semaphore.Wait();

            pbuffer.Return();
        }

        /// <inheritdoc />
        public virtual void CheckpointVersionShift(long oldVersion, long newVersion)
        {
        }
    }
}