// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    internal enum RecoveryPhase : byte { None = 0, Pass1 = 1 }

    /// <summary>
    /// Result of async page read
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public sealed class PageAsyncReadResult<TContext>
    {
        /// <summary>Index of the main-log page being read</summary>
        internal long page;

        /// <summary>Recovery device page offset</summary>
        internal int devicePageOffset;

        /// <summary>Context state to be passed through the read operation</summary>
        internal TContext context;

        /// <summary>Event to be signaled when the main-log page read is complete</summary>
        internal CountdownEvent handle;

        /// <summary>Callback to be called when the main-log page has completed processing; for <see cref="ObjectAllocator{TStoreFunctions}"/>
        /// this means after all Overflow or Objects on the page have been read as well.</summary>
        internal DeviceIOCompletionCallback callback;

        /// <summary>The destination pointer being read into.</summary>
        internal IntPtr destinationPtr;

        /// <summary>
        /// Main-log bytes below a snapshot suffix-merge boundary that share the first sector read from the snapshot file.
        /// Restored after the device read so only bytes at or above the logical merge boundary replace the recovered main page.
        /// </summary>
        internal SectorAlignedMemory preservedPagePrefix;
        internal IntPtr preservedPagePrefixDestination;
        internal int preservedPagePrefixLength;

        /// <summary>The cancellation token source, if any, for the Read operation</summary>
        internal CancellationTokenSource cts;

        /// <summary>Number of bytes read.</summary>
        public uint numBytesRead;

        /// <summary>Numeric error code returned by the device read callback.</summary>
        internal uint errorCode;

        /// <summary>Underlying exception returned by the device read callback, if any.</summary>
        internal Exception ioException;

        /// <summary>The max offset on the main log page to iterate records when determining how many bytes in the ObjectLog to read.</summary>
        internal long maxAddressOffsetOnPage;

        /// <summary>The recovery phase for this read. Non-<see cref="RecoveryPhase.None"/> uses the non-transient <see cref="ObjectIdMap"/>.</summary>
        internal RecoveryPhase recoveryPhase;

        /// <inheritdoc/>
        public override string ToString()
            => $"page {page}, recovPhase {recoveryPhase}, devPgOffset {devicePageOffset}, ctx {context}, countdown {handle?.CurrentCount}, destPtr {destinationPtr} (0x{destinationPtr:X}),"
             + $" maxPtr {maxAddressOffsetOnPage}, bytesRead {numBytesRead}, errorCode {errorCode}, ioException {ioException?.GetType().Name}";

        internal unsafe void RestorePreservedPagePrefix()
        {
            if (preservedPagePrefix is null)
                return;

            try
            {
                preservedPagePrefix.TotalValidSpan.Slice(0, preservedPagePrefixLength)
                    .CopyTo(new Span<byte>((void*)preservedPagePrefixDestination, preservedPagePrefixLength));
            }
            finally
            {
                preservedPagePrefix.Return();
                preservedPagePrefix = null;
                preservedPagePrefixDestination = IntPtr.Zero;
                preservedPagePrefixLength = 0;
            }
        }

        /// <summary>Release temporary read state.</summary>
        public void Free() => RestorePreservedPagePrefix();

        /// <inheritdoc/>
        public void DisposeHandle()
        {
            RestorePreservedPagePrefix();
            handle?.Dispose();
            handle = null;
        }
    }

    /// <summary>
    /// Shared flush completion tracker, when bulk-flushing many pages
    /// </summary>
    internal enum FlushRequestState : byte
    {
        /// <summary>The default; we are here for <see cref="AllocatorBase{TStoreFunctions, TAllocator}.AsyncFlushPagesForReadOnly"/> flush. This
        /// includes FoldOver checkpoints</summary>
        ReadOnly = 0,

        /// <summary>The flush is for <see cref="AllocatorBase{TStoreFunctions, TAllocator}.AsyncFlushPagesForRecovery"/>, so the object log files have
        /// already been written; we must reuse the deserialized object lengths to update the LogRecord's ObjectLogPosition rather than serialize again.</summary>
        Recovery,

        /// <summary>The flush is for <see cref="AllocatorBase{TStoreFunctions, TAllocator}.AsyncFlushPagesForSnapshot"/>, so we do not hold the epoch
        /// initially and thus must check to handle the case where HeadAddress increases out of the range of the flush</summary>
        Snapshot,

        /// <summary>A native-page Snapshot write was skipped because the page was already below HeadAddress.</summary>
        WriteNotIssued
    }

    /// <summary>
    /// Page async flush result
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public sealed class PageAsyncFlushResult<TContext>
    {
        /// <summary>The index of the log Page being written</summary>
        public int page;

        /// <summary>Context object for the callback</summary>
        public TContext context;

        /// <summary>The object log this page's out-of-line components are written to, if flushing ObjectAllocator; null when the page is
        /// written inline only.</summary>
        internal ObjectLog objectLog;

        /// <summary>Count of active pending flush operations; the callback decrements this and when it hits 0, the overall flush operation is complete.</summary>
        public int count;
        int firstErrorCode;

        /// <summary>If true, this is a flush of a partial page.</summary>
        internal bool partial;

        internal long fromAddress;
        internal long untilAddress;

        /// <summary>Identifes the operation that triggered the flush.</summary>
        internal FlushRequestState flushRequestState;

        /// <summary>The record buffer, passed through the IO process to retain a reference to it so it will not be GC'd before the Flush write completes.</summary>
        internal SectorAlignedMemory freeBuffer1;
        /// <summary>Optional second sector buffer for a split partial-page write.</summary>
        internal SectorAlignedMemory freeBuffer2;

        /// <summary>Set once the main snapshot device write for this page has been submitted (so its completion
        /// callback is guaranteed to fire and owns releasing this page's native snapshot-IO unit and buffers).
        /// Read by the issuing thread to decide, on a throw during issuance, whether it must release them itself
        /// (write never submitted) or defer to the callback (write submitted). Set and read only on the single
        /// FlushRunner issuing thread, so no memory barrier is needed. Used only by the native log-page backend.</summary>
        internal bool snapshotDeviceWriteIssued;

        /// <summary>Guards exactly-once release of this page's native snapshot-IO unit (the allocator's DirectVmBlockOwner count)
        /// across the completion callback and the issuer's exception path — the only overlap being the exotic case
        /// where the write was submitted, its callback ran (and released) synchronously, and then threw back out of
        /// the submit so the issuer also attempts release. 0 = not yet released. Used only by the native backend.</summary>
        int snapshotUnitReleased;

        /// <summary>Atomically claim the right to release this page's native snapshot-IO unit exactly once. Returns
        /// true to the single caller that should perform the release.</summary>
        internal bool TryClaimSnapshotUnitRelease() => Interlocked.Exchange(ref snapshotUnitReleased, 1) == 0;

        /// <summary>The event that is signaled by the callback so any waiting thread knows the IO has completed.</summary>
        internal AutoResetEvent done;

        internal FlushCompletionTracker flushCompletionTracker;

        /// <summary>Runtime-only Snapshot/ReadOnly page-ordering state. Set only for Snapshot page writes.</summary>
        internal SnapshotFlushCoordination snapshotFlushCoordination;

        /// <summary>During snapshot recovery, the snapshot object log that is the source for copying object bytes into the main object log
        /// (for records at/above <see cref="recoveryFormerFlushedUntilAddress"/>). Null for non-recovery flushes and for the hybrid-log region.</summary>
        internal ObjectLog recoverySnapshotObjectLog;

        /// <summary>During snapshot recovery, the former FlushedUntilAddress (the hybrid-log/snapshot boundary). Records whose logical address is at or
        /// above this are in the snapshot region and their objects must be copied from the snapshot object-log to the main object-log during the flush.</summary>
        internal long recoveryFormerFlushedUntilAddress;

        /// <summary>The checkpoint metadata version this recovery flush is rewriting (<see cref="HybridLogRecoveryInfo.hybridLogRecoveryVersion"/>). Selects
        /// the object-log record decode (downlevel cv7 vs current chunk-framed) for the records being flushed, instead of a per-record position-word flag.
        /// Defaults to the current version so non-recovery flushes never take a downlevel path.</summary>
        internal int checkpointVersion = HybridLogRecoveryInfo.CheckpointVersion;

        /// <inheritdoc/>
        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"page {page}, isFor {flushRequestState}, ctx {context}, count {count}, partial {bstr(partial)},"
                 + $" fromAddr {fromAddress} (0x{fromAddress:X}), untilAddr {untilAddress} (0x{untilAddress:X}),"
                 + $" flushCompTrack [{flushCompletionTracker}], objectLog [{objectLog}]";
        }

        /// <summary>
        /// Release our <see cref="count"/> and if it is zero, clear buffers.
        /// </summary>
        /// <returns>The decremented count</returns>
        /// <remarks>There is currently no AddRef(); count is assigned 1 at creation</remarks>
        internal int Release()
        {
            var result = Interlocked.Decrement(ref count);
            if (result == 0)
            {
                freeBuffer1?.Return();
                freeBuffer1 = null;
                freeBuffer2?.Return();
                freeBuffer2 = null;
                flushCompletionTracker?.CompleteOneFlush();
                flushCompletionTracker = null;
            }
            return result;
        }

        /// <summary>Retain the first error reported by any write span in this page batch.</summary>
        internal uint RecordError(uint errorCode)
        {
            if (errorCode != 0)
                _ = Interlocked.CompareExchange(ref firstErrorCode, unchecked((int)errorCode), 0);
            return unchecked((uint)Volatile.Read(ref firstErrorCode));
        }
    }

    /// <summary>
    /// Joins the parts of one page flush -- its device writes, and the durability of the object-log bytes its records reference -- so the
    /// page's callback runs exactly once, after all of them, with the first error any of them reported.
    /// </summary>
    /// <remarks>
    /// Once-only matters beyond counting: a Snapshot page's callback releases the page's native snapshot-IO unit on its first invocation,
    /// so a callback per part would release it while a write is still in flight. The issuer holds a sentinel part until it has submitted
    /// every part, so an early completion cannot finish the join.
    /// </remarks>
    internal sealed class PageFlushJoin
    {
        readonly DeviceIOCompletionCallback callback;
        readonly object context;
        int pending = 1;
        int firstErrorCode;
        Exception firstException;

        /// <summary>Create a join holding the issuer's sentinel part.</summary>
        /// <param name="callback">The page's flush callback.</param>
        /// <param name="context">The page's flush result, passed to <paramref name="callback"/>.</param>
        internal PageFlushJoin(DeviceIOCompletionCallback callback, object context)
        {
            this.callback = callback;
            this.context = context;
        }

        /// <summary>A completion callback for one part, whose context is the join.</summary>
        internal static readonly DeviceIOCompletionCallback PartCallback = static (errorCode, numBytes, context, ioException)
            => ((PageFlushJoin)context).Complete(errorCode, ioException);

        /// <summary>Add a part, before submitting it.</summary>
        internal void Add() => _ = Interlocked.Increment(ref pending);

        /// <summary>Complete a part, or the issuer's sentinel; the last completion invokes the page's callback.</summary>
        internal void Complete(uint errorCode, Exception ioException)
        {
            if (errorCode != 0 && Interlocked.CompareExchange(ref firstErrorCode, unchecked((int)errorCode), 0) == 0)
                firstException = ioException;
            if (Interlocked.Decrement(ref pending) == 0)
                callback(unchecked((uint)Volatile.Read(ref firstErrorCode)), numBytes: 0, context, firstException);
        }
    }

    /// <summary>
    /// Hold a <see cref="GCHandle"/> and a refcount; free the handle when the refcount reaches 0. Used when multiple sections of the
    /// same byte[] are being written, such as when it is split across segments.
    /// </summary>
    internal sealed class RefCountedPinnedGCHandle
    {
        /// <summary>The <see cref="GCHandle"/> being held.</summary>
        internal GCHandle gcHandle;
        /// <summary>Number of in-flight operations</summary>
        private long count;

        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"gcH {bstr(gcHandle.IsAllocated)}, count {count}";
        }

        internal RefCountedPinnedGCHandle(object targetObject, long initialCount)
        {
            gcHandle = GCHandle.Alloc(targetObject, GCHandleType.Pinned);
            count = initialCount;
        }

        internal RefCountedPinnedGCHandle(GCHandle gcHandle, long initialCount)
        {
            this.gcHandle = gcHandle;
            count = initialCount;
        }

        internal void AddRef()
        {
            ObjectDisposedException.ThrowIf(count <= 0, $"Uninitialized or final-released {nameof(RefCountedPinnedGCHandle)}");
            _ = Interlocked.Increment(ref count);
        }

        internal void Release()
        {
            ObjectDisposedException.ThrowIf(count <= 0, $"Uninitialized or final-released {nameof(RefCountedPinnedGCHandle)}");
            if (Interlocked.Decrement(ref count) == 0 && gcHandle.IsAllocated)
                gcHandle.Free();
        }

        internal object Target
        {
            get
            {
                ObjectDisposedException.ThrowIf(count <= 0 || !gcHandle.IsAllocated, $"Uninitialized or final-released {nameof(RefCountedPinnedGCHandle)}");
                return gcHandle.Target;
            }
        }

        internal bool IsAllocated => gcHandle.IsAllocated;
    }

    /// <summary>
    /// A class to carry callback and context through operations that may chain callbacks.
    /// </summary>
    internal sealed class DiskReadCallbackContext
    {
        /// <summary>If we had separate Reads directly into multiple spans of a single byte[], such as across segments, this is a refcounted wrapper for the <see cref="GCHandle"/>;
        /// it is released after the write and if it is the final release, all spans have been written and the GCHandle is freed (and the object unpinned).</summary>
        public RefCountedPinnedGCHandle refCountedGCHandle { get; private set; }

        /// <summary>Separate public Set() call so we ensure it is AddRef'd</summary>
        /// <param name="refGcHandle"></param>
        public void SetRefCountedHandle(RefCountedPinnedGCHandle refGcHandle)
        {
            Debug.Assert(!gcHandle.IsAllocated, "Cannot have both GCHandle and RefCountedPinnedGCHandle");
            refCountedGCHandle = refGcHandle;
            refCountedGCHandle.AddRef();
        }

        /// <summary>An event that can be waited for; the caller's callback will signal it if non-null.</summary>
        internal CountdownEvent countdownEvent;

        /// <summary>If we had a Read directly into the byte[] of an <see cref="OverflowByteArray"/>, this is the <see cref="GCHandle"/> that keps it pinned during the Read.
        /// After the Read it is freed (and the object unpinned).</summary>
        public GCHandle gcHandle;

        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"refCntGcH {refCountedGCHandle}, gcH {bstr(gcHandle.IsAllocated)}, countdown {countdownEvent?.CurrentCount}";
        }

        /// <summary>If non-null, this is the target buffer to copy data to (the copy is done by the caller's callback).</summary>
        public byte[] CopyTarget => (byte[])(gcHandle.IsAllocated ? gcHandle.Target : refCountedGCHandle.Target);

        internal DiskReadCallbackContext(CountdownEvent countdownEvent) => this.countdownEvent = countdownEvent;

        internal DiskReadCallbackContext(CountdownEvent countdownEvent, RefCountedPinnedGCHandle refGcHandle) : this(countdownEvent)
            => SetRefCountedHandle(refGcHandle);

        internal DiskReadCallbackContext(CountdownEvent countdownEvent, GCHandle gcHandle) : this(countdownEvent)
            => this.gcHandle = gcHandle;

        public void Dispose()
        {
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            _ = (countdownEvent?.Signal());
        }
    }
}