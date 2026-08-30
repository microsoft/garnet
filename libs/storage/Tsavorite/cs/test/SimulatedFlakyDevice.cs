// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test
{
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

        /// <summary>True once the <see cref="ThrowOnReadOrdinal"/> read has actually thrown, so a test can assert that
        /// its fault injection really fired rather than silently passing as a no-op.</summary>
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
    /// Completes reads through the IO callback with a non-zero error code rather than throwing. This is the shape a
    /// real device failure takes, and it is the only way to exercise callers that inspect the callback's error code
    /// instead of relying on an exception.
    /// </summary>
    public class ErrorCodeOnReadDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        /// <summary>When non-zero, reads complete with this error code and no data is transferred.</summary>
        public volatile uint ReadErrorCode;

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
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }
}