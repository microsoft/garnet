// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// A checkpoint manager that serves one preset index device. Only <see cref="GetIndexDevice"/> is reachable from
    /// the index recovery path; every other member throws so that an unexpected call is not silently ignored.
    /// </summary>
    public sealed class SingleDeviceCheckpointManager : ICheckpointManager
    {
        private readonly IDevice indexDevice;

        public SingleDeviceCheckpointManager(IDevice indexDevice) => this.indexDevice = indexDevice;

        /// <inheritdoc/>
        public IDevice GetIndexDevice(Guid indexToken) => indexDevice;

        /// <inheritdoc/>
        public bool PerformAutomaticCleanup => throw new NotSupportedException();
        /// <inheritdoc/>
        public byte[] GetCookie() => throw new NotSupportedException();
        /// <inheritdoc/>
        public void InitializeIndexCheckpoint(Guid indexToken) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void InitializeLogCheckpoint(Guid logToken) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void CleanupIndexCheckpoint(Guid indexToken) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void CommitLogCheckpointMetadata(Guid logToken, byte[] commitMetadata) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void CleanupLogCheckpoint(Guid logToken) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void CheckpointVersionShiftStart(long oldVersion, long newVersion, bool isStreaming) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void CheckpointVersionShiftEnd(long oldVersion, long newVersion, bool isStreaming) => throw new NotSupportedException();
        /// <inheritdoc/>
        public byte[] GetIndexCheckpointMetadata(Guid indexToken) => throw new NotSupportedException();
        /// <inheritdoc/>
        public byte[] GetLogCheckpointMetadata(Guid logToken) => throw new NotSupportedException();
        /// <inheritdoc/>
        public IEnumerable<Guid> GetIndexCheckpointTokens() => throw new NotSupportedException();
        /// <inheritdoc/>
        public IEnumerable<Guid> GetLogCheckpointTokens() => throw new NotSupportedException();
        /// <inheritdoc/>
        public IDevice GetSnapshotLogDevice(Guid token) => throw new NotSupportedException();
        /// <inheritdoc/>
        public IDevice GetSnapshotObjectLogDevice(Guid token) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void Purge(Guid token) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void PurgeAll() => throw new NotSupportedException();
        /// <inheritdoc/>
        public void OnRecovery(Guid indexToken, Guid logToken) => throw new NotSupportedException();
        /// <inheritdoc/>
        public void Dispose() { }
    }

    public class ErrorSimulationOptions
    {
        public double readTransientErrorRate;
        public double readPermanentErrorRate;
        public double writeTransientErrorRate;
        public double writePermanentErrorRate;
    }

    public class SimulatedFlakyDevice : StorageDeviceBase
    {
        private IDevice underlying;
        private ErrorSimulationOptions options;
        private ThreadLocal<Random> random;
        private List<long> permanentlyFailedRangesStart, permanentlyFailedRangesEnd;
        private EpochProtectedVersionScheme versionScheme;

        public SimulatedFlakyDevice(IDevice underlying, ErrorSimulationOptions options) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
        {
            this.underlying = underlying;
            this.options = options;
            permanentlyFailedRangesStart = new List<long>();
            permanentlyFailedRangesEnd = new List<long>();
            versionScheme = new EpochProtectedVersionScheme();
            random = new ThreadLocal<Random>(() => new Random());
        }

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            // Forward to both: our own base (so segment-size routing reflects the override) and
            // the wrapped device (so its segment-size geometry matches ours for IO routing).
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            underlying.RemoveSegmentAsync(segment, callback, result);
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            var logicalDestStart = segmentId * underlying.SegmentSize + (long)destinationAddress;
            var logicalDestEnd = logicalDestStart + numBytesToWrite;
            var state = versionScheme.Enter();
            try
            {
                if (permanentlyFailedRangesStart.Count != 0)
                {
                    // First failed range that's smaller than requested range start
                    var startIndex = permanentlyFailedRangesStart.BinarySearch(logicalDestStart);
                    if (startIndex < 0) startIndex = ~startIndex - 1;
                    // Start at 0 if smaller
                    startIndex = Math.Max(0, startIndex);

                    // check if there are overlaps
                    for (var i = startIndex; i < permanentlyFailedRangesStart.Count; i++)
                    {
                        if (permanentlyFailedRangesStart[i] > logicalDestEnd) break;
                        if (permanentlyFailedRangesEnd[i] > logicalDestStart)
                        {
                            // If so, simulate a failure by calling callback with an error
                            callback(42, numBytesToWrite, context, ioException: default);
                            return;
                        }
                    }
                }

                // Otherwise, decide whether we need to introduce a failure
                if (random.Value.NextDouble() < options.writeTransientErrorRate)
                {
                    // A device must complete each IO exactly once. Having signaled failure, return
                    // instead of also forwarding to the underlying device (which would deliver a second completion).
                    callback(42, numBytesToWrite, context, ioException: default);
                    return;
                }
                // decide whether failure should be in fact permanent. Don't necessarily need to fail concurrent requests
                else if (random.Value.NextDouble() < options.writePermanentErrorRate)
                {
                    callback(42, numBytesToWrite, context, ioException: default);
                    versionScheme.TryAdvanceVersionWithCriticalSection((_, _) =>
                    {
                        var index = permanentlyFailedRangesStart.BinarySearch(logicalDestStart);
                        if (index >= 0)
                            permanentlyFailedRangesEnd[index] =
                                Math.Max(permanentlyFailedRangesEnd[index], logicalDestEnd);
                        else
                        {
                            // This technically does not correctly merge / stores overlapping ranges, but for failing
                            // segments, it does not matter
                            var i = ~index;
                            permanentlyFailedRangesStart.Insert(i, logicalDestStart);
                            permanentlyFailedRangesEnd.Insert(i, logicalDestEnd);
                        }
                    });
                    return;
                }
            }
            finally
            {
                if (!state.IsError())
                    versionScheme.Leave();
            }
            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            var logicalSrcStart = segmentId * underlying.SegmentSize + (long)sourceAddress;
            var logicalSrcEnd = logicalSrcStart + readLength;
            var state = versionScheme.Enter();
            try
            {
                if (permanentlyFailedRangesStart.Count != 0)
                {
                    // First failed range that's smaller than requested range start
                    var startIndex = permanentlyFailedRangesStart.BinarySearch(logicalSrcStart);
                    if (startIndex < 0) startIndex = ~startIndex - 1;
                    // Start at 0 if smaller
                    startIndex = Math.Max(0, startIndex);

                    // check if there are overlaps
                    for (var i = startIndex; i < permanentlyFailedRangesStart.Count; i++)
                    {
                        if (permanentlyFailedRangesStart[i] > logicalSrcEnd) break;
                        if (permanentlyFailedRangesEnd[i] > logicalSrcStart)
                        {
                            // If so, simulate a failure by calling callback with an error
                            callback(42, readLength, context, ioException: default);
                            return;
                        }
                    }
                }
                // Otherwise, decide whether we need to introduce a failure
                if (random.Value.NextDouble() < options.readTransientErrorRate)
                {
                    // A device must complete each IO exactly once. Having signaled failure, return
                    // instead of also forwarding to the underlying device (which would deliver a second completion).
                    callback(42, readLength, context, ioException: default);
                    return;
                }
                else if (random.Value.NextDouble() < options.readPermanentErrorRate)
                {
                    callback(42, readLength, context, ioException: default);

                    versionScheme.TryAdvanceVersionWithCriticalSection((_, _) =>
                    {
                        var index = permanentlyFailedRangesStart.BinarySearch(logicalSrcStart);
                        if (index >= 0)
                            permanentlyFailedRangesEnd[index] =
                                Math.Max(permanentlyFailedRangesEnd[index], logicalSrcEnd);
                        else
                        {
                            var i = ~index;
                            permanentlyFailedRangesStart.Insert(i, logicalSrcStart);
                            permanentlyFailedRangesEnd.Insert(i, logicalSrcEnd);
                        }
                    });
                    return;
                }
            }
            finally
            {
                if (!state.IsError())
                    versionScheme.Leave();
            }

            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        public override void Dispose()
        {
            underlying.Dispose();
            versionScheme.Dispose();
        }
    }

    /// <summary>
    /// Device whose <see cref="ReadAsync"/> throws synchronously once armed, simulating a device that fails before
    /// the read is ever issued (e.g. native device creation failure, misalignment rejection, or use after dispose).
    /// No completion callback is delivered for such a read.
    /// </summary>
    public class SyncThrowOnReadDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        /// <summary>When true, reads throw synchronously instead of being issued.</summary>
        public volatile bool ArmReadFailure;

        /// <summary>
        /// When non-negative, only the read with this zero-based ordinal throws synchronously; every other read is
        /// issued normally. Lets a test fail one specific page read (e.g. a read-ahead) rather than all of them.
        /// </summary>
        public int ThrowOnReadOrdinal = -1;

        private int readOrdinal = -1;

        /// <summary>True once the <see cref="ThrowOnReadOrdinal"/> read has thrown, so a test can assert its fault
        /// injection fired.</summary>
        public volatile bool ReadFailureInjected;

        public SyncThrowOnReadDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            if (ArmReadFailure)
                throw new IOException("Simulated synchronous device read failure");
            if (ThrowOnReadOrdinal >= 0 && Interlocked.Increment(ref readOrdinal) == ThrowOnReadOrdinal)
            {
                ReadFailureInjected = true;
                throw new IOException($"Simulated synchronous device read failure on read ordinal {ThrowOnReadOrdinal}");
            }
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Wraps a device and throws synchronously from <see cref="WriteAsync"/> when armed, so a caller that fans one
    /// logical write out across several shards can be tested for correct cleanup of the shard that was never issued.
    /// </summary>
    public class SyncThrowOnWriteDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        /// <summary>When true, writes throw synchronously instead of being issued.</summary>
        public volatile bool ArmWriteFailure;

        public SyncThrowOnWriteDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            if (ArmWriteFailure)
                throw new IOException("Simulated synchronous device write failure");
            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
            => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Completes reads through the IO callback with a non-zero error code rather than throwing, exercising callers
    /// that inspect the callback's error code instead of relying on an exception.
    /// </summary>
    public class ErrorCodeOnReadDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        /// <summary>When non-zero, reads complete with this error code and no data is transferred.</summary>
        public volatile uint ReadErrorCode;

        /// <summary>When non-negative, reads succeed but report only this many bytes transferred, as a device does
        /// when the file ends before the requested length.</summary>
        public volatile int ShortReadBytes = -1;

        public ErrorCodeOnReadDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            var errorCode = ReadErrorCode;
            if (errorCode != 0)
            {
                callback(errorCode, 0, context, null);
                return;
            }

            var shortReadBytes = ShortReadBytes;
            if (shortReadBytes >= 0)
            {
                callback(0, (uint)shortReadBytes, context, null);
                return;
            }
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Simulates an operating system that caps a single transfer, as Linux does at MAX_RW_COUNT (INT_MAX rounded down
    /// to a page boundary): a request longer than <see cref="MaxBytesPerRequest"/> is issued to the underlying device
    /// for only that many bytes, and therefore completes successfully reporting the truncated count.
    /// </summary>
    public class TruncatingIoDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        /// <summary>Maximum number of bytes a single request transfers; longer requests complete short. Defaults to
        /// no truncation.</summary>
        public volatile uint MaxBytesPerRequest = uint.MaxValue;

        private int truncatedRequestCount;

        /// <summary>Number of requests that were truncated, so a test can assert its fault injection fired.</summary>
        public int TruncatedRequestCount => truncatedRequestCount;

        /// <summary>Offset and requested (pre-truncation) length of every write issued to this device, in issue order.</summary>
        public readonly ConcurrentQueue<(ulong offset, uint length)> Writes = new();

        /// <summary>Offset and requested (pre-truncation) length of every read issued to this device, in issue order.</summary>
        public readonly ConcurrentQueue<(ulong offset, uint length)> Reads = new();

        public TruncatingIoDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            Writes.Enqueue((destinationAddress, numBytesToWrite));
            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, Truncate(numBytesToWrite), callback, context);
        }

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            Reads.Enqueue((sourceAddress, readLength));
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, Truncate(readLength), callback, context);
        }

        private uint Truncate(uint numBytes)
        {
            var maxBytes = MaxBytesPerRequest;
            if (numBytes <= maxBytes)
                return numBytes;
            _ = Interlocked.Increment(ref truncatedRequestCount);
            return maxBytes;
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Simulates a device that completes transfers successfully but does not populate the transferred byte count,
    /// which <see cref="DeviceIOCompletionCallback"/> permits by reporting 0. Used to verify that short-transfer
    /// detection treats 0 as "not reported" rather than as a truncated transfer.
    /// </summary>
    public class ZeroCountReportingDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        public ZeroCountReportingDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, ZeroCount(callback), context);

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
            => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, ZeroCount(callback), context);

        private static DeviceIOCompletionCallback ZeroCount(DeviceIOCompletionCallback callback)
            => (errorCode, numBytes, context, ioException) => callback(errorCode, 0, context, ioException);

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Simulates a device whose request submission fails partway through a multi-request operation: the first
    /// <see cref="ThrowReadsAfter"/> reads are forwarded and the next submission throws synchronously, leaving the
    /// forwarded reads outstanding.
    /// </summary>
    public class ThrowOnNthReadDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;
        private int readCount;

        /// <summary>Number of reads to forward before the next submission throws.</summary>
        public int ThrowReadsAfter = int.MaxValue;

        /// <summary>Number of reads forwarded to the underlying device.</summary>
        public int ForwardedReadCount => readCount;

        public ThrowOnNthReadDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            if (Interlocked.Increment(ref readCount) > ThrowReadsAfter)
            {
                _ = Interlocked.Decrement(ref readCount);
                throw new IOException("Simulated submission failure");
            }
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Simulates a device whose write submission fails partway through a multi-request operation: the first
    /// <see cref="ThrowWritesAfter"/> writes are forwarded and the next submission throws synchronously, leaving the
    /// forwarded writes outstanding.
    /// </summary>
    public class ThrowOnNthWriteDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;
        private readonly ConcurrentQueue<Action> deferred = new();
        private int writeCount;

        /// <summary>Number of writes to forward before the next submission throws.</summary>
        public int ThrowWritesAfter = int.MaxValue;

        /// <summary>When set, the completion of every forwarded write is held until <see cref="CompleteDeferred"/>, so
        /// the writes issued before the failing submission stay in flight across it.</summary>
        public bool DeferWriteCompletions;

        /// <summary>Number of forwarded writes whose completion is still being held.</summary>
        public int DeferredCount => deferred.Count;

        public ThrowOnNthWriteDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
            => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            if (Interlocked.Increment(ref writeCount) > ThrowWritesAfter)
            {
                _ = Interlocked.Decrement(ref writeCount);
                throw new IOException("Simulated submission failure");
            }

            if (!DeferWriteCompletions)
            {
                underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
                return;
            }

            void Held(uint errorCode, uint numBytes, object ctx, Exception ex)
                => deferred.Enqueue(() => callback(errorCode, numBytes, ctx, ex));

            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, Held, context);
        }

        /// <summary>Invoke the write completions held back by <see cref="DeferWriteCompletions"/>.</summary>
        public void CompleteDeferred()
        {
            while (deferred.TryDequeue(out var complete))
                complete();
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Simulates a device that holds one read's completion until released, and records whether it was disposed while
    /// a completion was still outstanding. Used to verify that a failed recovery closes the checkpoint file only
    /// after every read it issued against that file has called back.
    /// </summary>
    public class DeferredCompletionDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;
        private readonly ConcurrentQueue<Action> deferred = new();
        private int readCount;
        private int outstanding;

        /// <summary>One-based index of the read whose completion is held until <see cref="CompleteDeferred"/>.</summary>
        public int DeferReadNumber = 1;

        /// <summary>Whether <see cref="Dispose"/> has run.</summary>
        public bool Disposed { get; private set; }

        /// <summary>Set if <see cref="Dispose"/> ran while a read had been issued but had not yet called back.</summary>
        public bool DisposedWithIoOutstanding { get; private set; }

        /// <summary>Number of reads whose completion is still being held.</summary>
        public int DeferredCount => deferred.Count;

        public DeferredCompletionDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            _ = Interlocked.Increment(ref outstanding);

            // Retire the request before handing control to the caller's callback: the caller's last callback is what
            // releases the waiter that then disposes this device, so the count must already be settled.
            void Wrapped(uint errorCode, uint numBytes, object ctx, Exception ex)
            {
                _ = Interlocked.Decrement(ref outstanding);
                callback(errorCode, numBytes, ctx, ex);
            }

            if (Interlocked.Increment(ref readCount) == DeferReadNumber)
            {
                deferred.Enqueue(() => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, Wrapped, context));
                return;
            }
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, Wrapped, context);
        }

        /// <summary>Submit the held reads.</summary>
        public void CompleteDeferred()
        {
            while (deferred.TryDequeue(out var submit))
                submit();
        }

        /// <inheritdoc/>
        public override void Dispose()
        {
            if (Volatile.Read(ref outstanding) > 0)
                DisposedWithIoOutstanding = true;
            Disposed = true;
            underlying.Dispose();
        }
    }

    /// <summary>
    /// Simulates a device whose transfers fail asynchronously, reporting the failure through
    /// <see cref="DeviceIOCompletionCallback"/> with both an error code and the originating exception, as the local
    /// storage devices do. Used to verify that the exception survives to the caller as an inner exception.
    /// </summary>
    public class CallbackExceptionDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        /// <summary>The exception reported to the completion callback of every failed request.</summary>
        public readonly IOException Injected = new("Simulated device failure");

        /// <summary>Whether to fail reads; when false, reads are forwarded to the underlying device.</summary>
        public bool FailReads;

        /// <summary>Whether to fail writes; when false, writes are forwarded to the underlying device.</summary>
        public bool FailWrites;

        public CallbackExceptionDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            if (FailWrites)
                callback(uint.MaxValue, 0, context, Injected);
            else
                underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            if (FailReads)
                callback(uint.MaxValue, 0, context, Injected);
            else
                underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Simulates a device that defers the completion of its first reads and then, on the next read, invokes the
    /// completion callback synchronously before throwing out of the submit. Used to verify that a request retired by
    /// its own callback is not retired a second time by the submission-failure path.
    /// </summary>
    public class CallbackThenThrowDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;
        private readonly ConcurrentQueue<Action> deferred = new();
        private int readCount;

        /// <summary>Number of initial reads to capture without completing, leaving them outstanding.</summary>
        public int DeferReadsBefore = int.MaxValue;

        /// <summary>Number of reads whose completion is still pending.</summary>
        public int DeferredCount => deferred.Count;

        public CallbackThenThrowDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <inheritdoc/>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            if (Interlocked.Increment(ref readCount) <= DeferReadsBefore)
            {
                // Hold this read open: it stays outstanding until CompleteDeferred is called.
                deferred.Enqueue(() => callback(0, readLength, context, null));
                return;
            }

            // Complete this read and then fail the submit, the interleaving the one-shot retirement guard exists for.
            callback(0, readLength, context, null);
            throw new IOException("Simulated submission failure after synchronous completion");
        }

        /// <summary>Complete every read that was held open.</summary>
        public void CompleteDeferred()
        {
            while (deferred.TryDequeue(out var complete))
                complete();
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }
}