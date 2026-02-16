// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
                            callback(42, numBytesToWrite, context);
                            return;
                        }
                    }
                }

                // Otherwise, decide whether we need to introduce a failure
                if (random.Value.NextDouble() < options.writeTransientErrorRate)
                {
                    callback(42, numBytesToWrite, context);
                }
                // decide whether failure should be in fact permanent. Don't necessarily need to fail concurrent requests
                else if (random.Value.NextDouble() < options.writePermanentErrorRate)
                {
                    callback(42, numBytesToWrite, context);
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
                            callback(42, readLength, context);
                            return;
                        }
                    }
                }
                // Otherwise, decide whether we need to introduce a failure
                if (random.Value.NextDouble() < options.readTransientErrorRate)
                {
                    callback(42, readLength, context);
                }
                else if (random.Value.NextDouble() < options.readPermanentErrorRate)
                {
                    callback(42, readLength, context);

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
}