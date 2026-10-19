// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Device factory creator that holds the store hybrid log's read completions for a settable delay, turning
    /// a record that has fallen out of memory into a slow, deterministic device read, and can hold its write
    /// completions to stall its flushes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Injected by assigning it to <c>GarnetServerOptions.DeviceFactoryCreator</c>. Only the store hybrid log
    /// is wrapped: checkpoint devices and the AOF keep their normal timing, as do its writes unless
    /// <see cref="HoldWrites"/> is set, so the server starts, flushes and shuts down at full speed and the delay
    /// applies to exactly the operation under test.
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
        readonly PriorityQueue<PendingRead, long> pending = new();
        readonly List<HeldWrite> heldWrites = [];
        readonly ConcurrentQueue<PendingRead> pool = new();
        readonly Thread scheduler;
        bool stopped;

        /// <summary>
        /// One in-flight read's completion arguments, rented for the duration of the read and returned once
        /// the callback has been handed back to Tsavorite.
        /// </summary>
        /// <remarks>
        /// This exists so that wrapping a read costs no allocation. A test here measures the allocation of a
        /// GET that goes to the device, and a wrapper that captured the callback in a closure would put its
        /// own cost - two closures and two delegates per read - inside the number under test and bury what
        /// the server actually spends.
        /// </remarks>
        sealed class PendingRead
        {
            internal DelayedReadDeviceFactoryCreator owner;
            internal DeviceIOCompletionCallback callback;
            internal object context;
            internal uint errorCode;
            internal uint numBytes;
            internal Exception exception;
        }

        /// <summary>
        /// Passed to the underlying device in place of the real callback. Static, so it is allocated once:
        /// everything that varies per read travels in the <see cref="PendingRead"/> handed over as the
        /// device's context.
        /// </summary>
        static readonly DeviceIOCompletionCallback trampoline = static (errorCode, numBytes, context, exception) =>
        {
            var read = (PendingRead)context;
            read.errorCode = errorCode;
            read.numBytes = numBytes;
            read.exception = exception;
            read.owner.Delay(read);
        };

        /// <summary>A write's completion, withheld while <see cref="HoldWrites"/> is set.</summary>
        readonly record struct HeldWrite(DeviceIOCompletionCallback Callback, uint ErrorCode, uint NumBytes, object Context, Exception Exception);

        /// <summary>
        /// Set to withhold the completion of every store hybrid log write issued from then on until <see cref="ReleaseWrites"/>,
        /// so the log's flushed address stops advancing. The data is written; only its completion is withheld.
        /// </summary>
        internal volatile bool HoldWrites;

        /// <summary>Number of write completions withheld and not yet released.</summary>
        internal int WritesHeld
        {
            get
            {
                lock (gate)
                    return heldWrites.Count;
            }
        }

        /// <summary>Clears <see cref="HoldWrites"/> and completes every write it withheld.</summary>
        internal void ReleaseWrites()
        {
            HeldWrite[] released;
            lock (gate)
            {
                HoldWrites = false;
                released = [.. heldWrites];
                heldWrites.Clear();
            }

            foreach (var write in released)
                write.Callback(write.ErrorCode, write.NumBytes, write.Context, write.Exception);
        }

        /// <summary>Wraps <paramref name="callback"/> to be withheld if <see cref="HoldWrites"/> is set when it is issued.</summary>
        DeviceIOCompletionCallback WrapWrite(DeviceIOCompletionCallback callback)
            => !HoldWrites ? callback : (errorCode, numBytes, context, exception) =>
            {
                lock (gate)
                {
                    if (HoldWrites && !stopped)
                    {
                        heldWrites.Add(new(callback, errorCode, numBytes, context, exception));
                        return;
                    }
                }

                callback(errorCode, numBytes, context, exception);
            };

        /// <summary>Takes a record for a read that is about to be issued, and counts the read.</summary>
        PendingRead Rent(DeviceIOCompletionCallback callback, object context, bool objectLog)
        {
            _ = Interlocked.Increment(ref reads);
            if (objectLog)
                _ = Interlocked.Increment(ref objectLogReads);
            if (!inCompletion && !Thread.CurrentThread.IsThreadPoolThread)
                _ = Interlocked.Increment(ref readsOffThreadPool);

            if (!pool.TryDequeue(out var read))
                read = new PendingRead { owner = this };
            read.callback = callback;
            read.context = context;
            return read;
        }

        /// <summary>Returns <paramref name="read"/> to the pool and then completes it.</summary>
        static void Complete(PendingRead read)
        {
            var callback = read.callback;
            var context = read.context;
            var errorCode = read.errorCode;
            var numBytes = read.numBytes;
            var exception = read.exception;

            // Returned before the callback runs, so a callback that issues another read reuses this record
            // rather than growing the pool, and so a callback that throws does not leak it.
            read.callback = null;
            read.context = null;
            read.exception = null;
            read.owner.pool.Enqueue(read);

            var wasInCompletion = inCompletion;
            inCompletion = true;
            try
            {
                callback(errorCode, numBytes, context, exception);
            }
            finally
            {
                inCompletion = wasInCompletion;
            }
        }

        /// <summary>
        /// Set while a read's completion callback runs, which is where a scan issues the read for the next record.
        /// </summary>
        [ThreadStatic]
        static bool inCompletion;

        int reads;
        int objectLogReads;
        int readsOffThreadPool;

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

        /// <summary>
        /// Number of the reads counted by <see cref="ReadCount"/> that went to the store's object log rather than its main
        /// log.
        /// </summary>
        internal int ObjectLogReadCount => Volatile.Read(ref objectLogReads);

        /// <summary>
        /// Number of store hybrid log reads issued since the last <see cref="ResetReadCount"/> by a command running on a
        /// thread other than a thread-pool thread, outside a read's completion. A command that completes its reads on the
        /// thread it runs on has to leave such a thread, so for such a command this stays at zero; a command that parks on
        /// its reads issues them wherever it runs.
        /// </summary>
        internal int ReadsOffThreadPool => Volatile.Read(ref readsOffThreadPool);

        internal DelayedReadDeviceFactoryCreator()
        {
            scheduler = new Thread(SchedulerLoop)
            {
                IsBackground = true,
                Name = "DelayedReadDeviceCompletions",
            };
            scheduler.Start();
        }

        /// <summary>
        /// Clears <see cref="ReadCount"/>, <see cref="ObjectLogReadCount"/> and <see cref="ReadsOffThreadPool"/>, discounting
        /// reads from populating the store.
        /// </summary>
        internal void ResetReadCount()
        {
            Volatile.Write(ref reads, 0);
            Volatile.Write(ref objectLogReads, 0);
            Volatile.Write(ref readsOffThreadPool, 0);
        }

        /// <summary>
        /// Completes every read currently withheld, however much of its delay is left. A test holds reads by arming
        /// a delay longer than it will wait, and releases them by setting <see cref="ReadDelayMs"/> back to zero
        /// and calling this.
        /// </summary>
        internal void CompleteHeldReads()
        {
            lock (gate)
            {
                var held = new List<PendingRead>(pending.Count);
                while (pending.TryDequeue(out var read, out _))
                    held.Add(read);

                foreach (var read in held)
                    pending.Enqueue(read, 0);

                Monitor.Pulse(gate);
            }
        }

        /// <summary>
        /// Completes every read currently withheld on the calling thread, so a read still completes when the thread that
        /// completes delayed reads is itself waiting for one. A test calls this on failure, to release such a wait.
        /// </summary>
        internal void CompleteHeldReadsHere()
        {
            while (true)
            {
                PendingRead read;
                lock (gate)
                {
                    if (!pending.TryDequeue(out read, out _))
                        return;
                }

                Complete(read);
            }
        }

        /// <inheritdoc/>
        public INamedDeviceFactory Create(string baseName) => new DelayedReadNamedDeviceFactory(inner.Create(baseName), this);

        public void Dispose()
        {
            ReleaseWrites();

            lock (gate)
            {
                stopped = true;
                Monitor.PulseAll(gate);
            }

            _ = scheduler.Join(TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// Completes <paramref name="read"/> after the currently armed delay, or immediately if there is none.
        /// </summary>
        void Delay(PendingRead read)
        {
            var delayMs = ReadDelayMs;
            if (delayMs <= 0)
            {
                Complete(read);
                return;
            }

            lock (gate)
            {
                // A completion queued after Dispose still has to run: Tsavorite is holding an IO request open
                // for it, and the store cannot be disposed until that request is retired.
                if (stopped)
                {
                    Complete(read);
                    return;
                }

                pending.Enqueue(read, Environment.TickCount64 + delayMs);
                Monitor.Pulse(gate);
            }
        }

        void SchedulerLoop()
        {
            while (true)
            {
                PendingRead next;
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
                    Complete(next);
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
        /// True for the store's object log, the part of its hybrid log that holds the serialized form of its objects.
        /// </summary>
        static bool IsStoreObjectLog(FileDescriptor fileInfo)
            => fileInfo.fileName is not null && fileInfo.fileName.StartsWith("hlog_objs", StringComparison.Ordinal);

        /// <summary>
        /// Delegating device factory that wraps the store hybrid log device and passes everything else through.
        /// </summary>
        private sealed class DelayedReadNamedDeviceFactory(INamedDeviceFactory underlying, DelayedReadDeviceFactoryCreator owner) : INamedDeviceFactory
        {
            /// <inheritdoc/>
            public IDevice Get(FileDescriptor fileInfo)
            {
                var device = underlying.Get(fileInfo);
                return IsStoreHybridLog(fileInfo) ? new DelayedReadDevice(device, owner, IsStoreObjectLog(fileInfo)) : device;
            }

            /// <inheritdoc/>
            public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);

            /// <inheritdoc/>
            public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);
        }

        /// <summary>
        /// Delegating device that withholds read completion callbacks for its owner's current delay, and write completion
        /// callbacks while its owner holds writes.
        /// </summary>
        private sealed class DelayedReadDevice(IDevice underlying, DelayedReadDeviceFactoryCreator owner, bool objectLog) : IDevice
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
                => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, owner.WrapWrite(callback), context);

            /// <inheritdoc/>
            public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
                => underlying.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, owner.WrapWrite(callback), context);

            /// <inheritdoc/>
            public void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, trampoline, owner.Rent(callback, context, objectLog));

            /// <inheritdoc/>
            public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint alignedReadLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(alignedSourceAddress, alignedDestinationAddress, alignedReadLength, trampoline, owner.Rent(callback, context, objectLog));

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
        }
    }
}