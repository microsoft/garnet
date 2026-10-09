// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Device factory creator that holds the store hybrid log's read completions for a settable delay, turning
    /// a record that has fallen out of memory into a slow, deterministic device read.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Injected by assigning it to <c>GarnetServerOptions.DeviceFactoryCreator</c>. Only the store hybrid log
    /// is wrapped, and only its reads: writes, checkpoint devices and the AOF keep their normal timing, so the
    /// server starts, flushes and shuts down at full speed and the delay applies to exactly the operation
    /// under test.
    /// </para>
    /// <para>
    /// The delayed callbacks are run by one dedicated thread rather than by a timer or a
    /// <see cref="System.Threading.Tasks.Task"/> continuation. Both of those would need a thread-pool thread
    /// to fire, which is the resource the tests here deliberately exhaust: a read completion that queued
    /// behind the very reads that are waiting for it would deadlock instead of exposing the thread starvation
    /// it is meant to demonstrate. The dedicated thread also removes the head-of-line serialization a device
    /// with a fixed number of IO threads would impose, so any number of reads can be in flight at once.
    /// </para>
    /// </remarks>
    internal sealed class DelayedReadDeviceFactoryCreator : INamedDeviceFactoryCreator, IDisposable
    {
        readonly INamedDeviceFactoryCreator inner = new LocalStorageNamedDeviceFactoryCreator();

        readonly object gate = new();
        readonly PriorityQueue<Action, long> pending = new();
        readonly Thread scheduler;
        bool stopped;

        int reads;

        /// <summary>
        /// How long each store hybrid log read completion is withheld, in milliseconds. Read on every
        /// completion, so the delay can be armed after the data has been written and the log flushed.
        /// Zero completes inline, at the device's own speed.
        /// </summary>
        internal volatile int ReadDelayMs;

        /// <summary>
        /// Number of store hybrid log reads issued since the last <see cref="ResetReadCount"/>. A test asserts
        /// on this to show that the records it read really did come from the device rather than from memory.
        /// </summary>
        internal int ReadCount => Volatile.Read(ref reads);

        internal DelayedReadDeviceFactoryCreator()
        {
            scheduler = new Thread(SchedulerLoop)
            {
                IsBackground = true,
                Name = "DelayedReadDeviceCompletions",
            };
            scheduler.Start();
        }

        /// <summary>Clears <see cref="ReadCount"/>, discounting reads from populating the store.</summary>
        internal void ResetReadCount() => Volatile.Write(ref reads, 0);

        /// <inheritdoc/>
        public INamedDeviceFactory Create(string baseName) => new DelayedReadNamedDeviceFactory(inner.Create(baseName), this);

        public void Dispose()
        {
            lock (gate)
            {
                stopped = true;
                Monitor.PulseAll(gate);
            }

            _ = scheduler.Join(TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// Runs <paramref name="completion"/> after the currently armed delay, or immediately if there is none.
        /// </summary>
        void Delay(Action completion)
        {
            var delayMs = ReadDelayMs;
            if (delayMs <= 0)
            {
                completion();
                return;
            }

            lock (gate)
            {
                // A completion queued after Dispose still has to run: Tsavorite is holding an IO request open
                // for it, and the store cannot be disposed until that request is retired.
                if (stopped)
                {
                    completion();
                    return;
                }

                pending.Enqueue(completion, Environment.TickCount64 + delayMs);
                Monitor.Pulse(gate);
            }
        }

        void SchedulerLoop()
        {
            while (true)
            {
                Action next;
                lock (gate)
                {
                    while (true)
                    {
                        // On shutdown, fire everything still queued rather than abandoning in-flight reads.
                        if (stopped)
                        {
                            if (!pending.TryDequeue(out next, out _))
                                return;
                            break;
                        }

                        if (!pending.TryPeek(out _, out var dueAt))
                        {
                            _ = Monitor.Wait(gate);
                            continue;
                        }

                        var waitMs = dueAt - Environment.TickCount64;
                        if (waitMs <= 0)
                        {
                            next = pending.Dequeue();
                            break;
                        }

                        _ = Monitor.Wait(gate, (int)Math.Min(waitMs, int.MaxValue));
                    }
                }

                try
                {
                    next();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Delayed read completion threw: {ex}");
                }
            }
        }

        /// <summary>
        /// True for the store's hybrid log, which is the only device whose reads are delayed.
        /// </summary>
        static bool IsStoreHybridLog(FileDescriptor fileInfo)
            => fileInfo.fileName is not null && fileInfo.fileName.StartsWith("hlog", StringComparison.Ordinal);

        /// <summary>
        /// Delegating device factory that wraps the store hybrid log device and passes everything else through.
        /// </summary>
        private sealed class DelayedReadNamedDeviceFactory(INamedDeviceFactory underlying, DelayedReadDeviceFactoryCreator owner) : INamedDeviceFactory
        {
            /// <inheritdoc/>
            public IDevice Get(FileDescriptor fileInfo)
            {
                var device = underlying.Get(fileInfo);
                return IsStoreHybridLog(fileInfo) ? new DelayedReadDevice(device, owner) : device;
            }

            /// <inheritdoc/>
            public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);

            /// <inheritdoc/>
            public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);
        }

        /// <summary>
        /// Delegating device that withholds read completion callbacks for its owner's current delay.
        /// </summary>
        private sealed class DelayedReadDevice(IDevice underlying, DelayedReadDeviceFactoryCreator owner) : IDevice
        {
            /// <inheritdoc/>
            public uint SectorSize => underlying.SectorSize;

            /// <inheritdoc/>
            public string FileName => underlying.FileName;

            /// <inheritdoc/>
            public long Capacity => underlying.Capacity;

            /// <inheritdoc/>
            public long SegmentSize => underlying.SegmentSize;

            /// <inheritdoc/>
            public int StartSegment => underlying.StartSegment;

            /// <inheritdoc/>
            public int EndSegment => underlying.EndSegment;

            /// <inheritdoc/>
            public int ThrottleLimit { get => underlying.ThrottleLimit; set => underlying.ThrottleLimit = value; }

            /// <inheritdoc/>
            public void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
                => underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);

            /// <inheritdoc/>
            public bool TryComplete() => underlying.TryComplete();

            /// <inheritdoc/>
            public bool Throttle() => underlying.Throttle();

            /// <inheritdoc/>
            public void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
                => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

            /// <inheritdoc/>
            public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
                => underlying.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, callback, context);

            /// <inheritdoc/>
            public void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, Delayed(callback), context);

            /// <inheritdoc/>
            public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint alignedReadLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(alignedSourceAddress, alignedDestinationAddress, alignedReadLength, Delayed(callback), context);

            /// <inheritdoc/>
            public void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
                => underlying.TruncateUntilAddressAsync(toAddress, callback, result);

            /// <inheritdoc/>
            public void TruncateUntilAddress(long toAddress) => underlying.TruncateUntilAddress(toAddress);

            /// <inheritdoc/>
            public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
                => underlying.TruncateUntilSegmentAsync(toSegment, callback, result);

            /// <inheritdoc/>
            public void TruncateUntilSegment(int toSegment) => underlying.TruncateUntilSegment(toSegment);

            /// <inheritdoc/>
            public void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
                => underlying.RemoveSegmentAsync(segment, callback, result);

            /// <inheritdoc/>
            public void RemoveSegment(int segment) => underlying.RemoveSegment(segment);

            /// <inheritdoc/>
            public long GetFileSize(int segment) => underlying.GetFileSize(segment);

            /// <inheritdoc/>
            public void Reset() => underlying.Reset();

            /// <inheritdoc/>
            public void Dispose() => underlying.Dispose();

            DeviceIOCompletionCallback Delayed(DeviceIOCompletionCallback callback)
            {
                _ = Interlocked.Increment(ref owner.reads);
                return (errorCode, numBytes, context, exception)
                    => owner.Delay(() => callback(errorCode, numBytes, context, exception));
            }
        }
    }
}