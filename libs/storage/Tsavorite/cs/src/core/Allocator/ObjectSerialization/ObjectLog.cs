// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Configuration for an <see cref="ObjectLog"/>.
    /// </summary>
    internal sealed class ObjectLogSettings
    {
        /// <summary>The device holding the object-log bytes. Required.</summary>
        internal IDevice LogDevice;

        /// <summary>Page size in bits. A page bounds a single allocation, so this is also the largest chunk the writer can produce.</summary>
        /// <remarks>Defaults to <see cref="ObjectLog.DefaultPageSizeBits"/>. Must be settable far below that for tests, which is why
        /// the only floor enforced here is <see cref="LogSettings.kMinPageSizeBits"/>; the relation to <see cref="IStreamBuffer.BufferSize"/>
        /// is the writer's constraint, not the log's.</remarks>
        internal int PageSizeBits = ObjectLog.DefaultPageSizeBits;

        /// <summary>Number of pages in the circular buffer.</summary>
        internal int PageCount = ObjectLog.DefaultPageCount;

        /// <summary>Device segment size in bits. This is file segmentation only: it is not part of an object-log address.</summary>
        internal int SegmentSizeBits = ObjectLog.DefaultSegmentSizeBits;

        /// <summary>Fraction of the in-memory region left mutable.</summary>
        internal double MutableFraction = 0.9;

        /// <summary>Test-only: lower the address ceiling so <see cref="ObjectLog"/>'s enforcement can be exercised. Zero uses the real
        /// ceiling, which no test can reach -- it is billions of pages away.</summary>
        internal long MaxExclusiveAddressOverride;

        /// <summary>Epoch to use. When null, the log creates and owns one.</summary>
        internal LightEpoch Epoch;

        /// <summary>Logger, or null.</summary>
        internal ILogger Logger;
    }

    /// <summary>
    /// A log-structured store for serialized object (and overflow key) bytes, composed over <see cref="TsavoriteLogAllocatorImpl"/>.
    /// </summary>
    /// <remarks>
    /// <para>This replaces the hand-managed object-log file. The allocator supplies concurrent allocation, page flushing, eviction and
    /// truncation, so the object log no longer tracks segments and offsets itself; an object-log position is a flat logical address.</para>
    /// <para><b>No commit protocol.</b> Unlike <see cref="TsavoriteLog"/> there is no commit file, no commit record and no recovery of a
    /// "latest commit": the checkpoint's <c>info.dat</c> remains the sole authority on object-log tails, and recovery positions this log
    /// with <see cref="RestoreTail"/>. Durability is a wait on <see cref="FlushedUntilAddress"/>.</para>
    /// <para><b>Scope discipline.</b> Allocation and the fill that follows it must happen inside one <see cref="BeginScope"/> /
    /// <see cref="EndScope"/> pair, because a physical address is only stable while the epoch is held. A physical address must not be
    /// retained across a later <see cref="Allocate(int)"/> call on the same scope: an allocation that has to wait for a flush suspends
    /// the epoch, after which the earlier page may be closed. Bytes already written there are safe -- they are on disk by then -- but the
    /// pointer is not.</para>
    /// </remarks>
    internal sealed unsafe class ObjectLog : IDisposable
    {
        /// <summary>Default page size in bits: twice <see cref="IStreamBuffer.BufferSize"/>, so a full buffer fits a page with room to
        /// spare and page-tail cuts are occasional rather than near-universal.</summary>
        internal const int DefaultPageSizeBits = 23;    // 8 MB

        /// <summary>Default number of pages. One writer fills the tail page while earlier pages flush; eight pages at
        /// <see cref="DefaultPageSizeBits"/> let several page flushes overlap the writer before an allocation must wait for one.</summary>
        internal const int DefaultPageCount = 8;

        /// <summary>Minimum number of pages, leaving the writer room to fill a page while earlier pages flush. Tests shrink the page
        /// size rather than the page count.</summary>
        internal const int MinPageCount = 4;

        /// <summary>Maximum number of pages. <see cref="ComputeMaxExclusiveAddress"/> reserves the page ring, rounded up to a power of 2,
        /// as headroom below <see cref="int.MaxValue"/> page numbers; a larger ring would round up to 2^31 and leave no page numbers.</summary>
        internal const int MaxPageCount = 1 << 30;

        /// <summary>Default device segment size in bits; file segmentation only.</summary>
        internal const int DefaultSegmentSizeBits = 33; // 8 GB

        /// <summary>
        /// One past the highest address this log may ever hand out.
        /// </summary>
        /// <remarks>
        /// Two independent ceilings bound an object-log address, and this is the lower of them.
        /// <list type="bullet">
        /// <item><description>An object-log position must fit the address field of <see cref="ObjectLogPositionInfo"/>, whose
        /// remaining bits are flags, so addresses stop below <see cref="ObjectLogPositionInfo.AddressMask"/>. The all-ones
        /// value itself is excluded because it is the unstamped marker (see
        /// <see cref="ObjectLogPositionInfo.WordIsUnstamped"/>).</description></item>
        /// <item><description>The allocator's tail is a <c>PageOffset</c> whose page number is a signed 32-bit field, so it runs out
        /// after <see cref="int.MaxValue"/> pages.</description></item>
        /// </list>
        /// The page-number ceiling is always the binding one at the page sizes this log permits: it reaches the 60-bit position ceiling
        /// only at a page of 2^29 bytes, above <see cref="LogSettings.kMaxPageSizeBits"/>. Both are checked through this single value so
        /// a future page-size change cannot quietly swap which one applies.
        /// <para>The page-number term reserves headroom above the highest allocatable page rather than running to
        /// <see cref="int.MaxValue"/>. The allocator computes <c>pageIndex + 1</c> and <c>BufferSize + GetPage(...)</c> on signed
        /// 32-bit page numbers while deciding whether a page can be reused; those additions overflow near the limit and misclassify a
        /// page, and they happen inside the allocator, below this class, where the ceiling check cannot observe them.</para>
        /// <para>Truncating into the flag bits, or wrapping the page number, would be silent corruption, so this is enforced on every
        /// allocation's end address rather than left to arithmetic.</para>
        /// </remarks>
        internal readonly long MaxExclusiveAddress;

        private readonly TsavoriteLogAllocatorImpl allocator;
        private readonly LightEpoch epoch;
        private readonly bool isEpochOwned;
        private readonly IDevice device;
        private readonly ILogger logger;
        private bool disposed;

        /// <summary>Callers waiting, through <see cref="OnFlushed"/>, for <see cref="FlushedUntilAddress"/> to reach an address.</summary>
        private readonly List<FlushWaiter> flushWaiters = [];
        private readonly object flushWaitersLock = new();

        /// <summary>The earliest flush failure reported by the allocator; null until one occurs. A failed flush never advances
        /// <see cref="FlushedUntilAddress"/>, so a later wait past it fails at once.</summary>
        private CommitInfo? flushFailure;

        private readonly struct FlushWaiter(long untilAddress, DeviceIOCompletionCallback callback, object context)
        {
            internal readonly long UntilAddress = untilAddress;
            internal readonly DeviceIOCompletionCallback Callback = callback;
            internal readonly object Context = context;
        }

        /// <summary>Create an object log over <paramref name="settings"/>'s device.</summary>
        internal ObjectLog(ObjectLogSettings settings)
        {
            device = settings.LogDevice ?? throw new TsavoriteException($"{nameof(ObjectLogSettings)}.{nameof(ObjectLogSettings.LogDevice)} must be specified");
            logger = settings.Logger;

            if (settings.PageSizeBits < LogSettings.kMinPageSizeBits || settings.PageSizeBits > LogSettings.kMaxPageSizeBits)
                throw new TsavoriteException($"{nameof(ObjectLogSettings.PageSizeBits)} ({settings.PageSizeBits}) must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
            if (settings.PageCount < MinPageCount || settings.PageCount > MaxPageCount)
                throw new TsavoriteException($"{nameof(ObjectLogSettings.PageCount)} ({settings.PageCount}) must be between {MinPageCount} and {MaxPageCount}");
            if (settings.SegmentSizeBits < settings.PageSizeBits || settings.SegmentSizeBits > LogSettings.kMaxSegmentSizeBits)
                throw new TsavoriteException($"{nameof(ObjectLogSettings.SegmentSizeBits)} ({settings.SegmentSizeBits}) must be between {nameof(ObjectLogSettings.PageSizeBits)} ({settings.PageSizeBits}) and {LogSettings.kMaxSegmentSizeBits}");

            if (settings.Epoch is null)
            {
                epoch = new LightEpoch();
                isEpochOwned = true;
            }
            else
                epoch = settings.Epoch;

            MaxExclusiveAddress = settings.MaxExclusiveAddressOverride > 0
                ? settings.MaxExclusiveAddressOverride
                : ComputeMaxExclusiveAddress(settings.PageSizeBits, settings.PageCount);

            var logSettings = new LogSettings
            {
                LogDevice = device,
                PageSizeBits = settings.PageSizeBits,
                SegmentSizeBits = settings.SegmentSizeBits,
                PageCount = settings.PageCount,
                MemorySize = (long)settings.PageCount << settings.PageSizeBits,
                MutableFraction = settings.MutableFraction,
                ObjectLogDevice = null,
                UpgradeObjectLogDevice = null,
                ReadCacheSettings = null,
                ReadCopyOptions = ReadCopyOptions.None,
            };

            try
            {
                allocator = new TsavoriteLogAllocatorImpl(new AllocatorSettings(logSettings, epoch, settings.Logger) { flushCallback = OnAllocatorFlush });
                allocator.Initialize();
            }
            catch
            {
                // The constructor is about to throw, so this instance's Dispose will never run; release what was taken above.
                // The epoch release is in a finally because a throwing allocator Dispose would otherwise leak one of the process-wide
                // LightEpoch instance slots on every failed construction.
                try
                {
                    allocator?.Dispose();
                }
                finally
                {
                    if (isEpochOwned)
                        epoch.Dispose();
                }
                throw;
            }
        }

        #region Geometry and addresses
        /// <summary>Size of a page in bytes. An allocation never spans pages, so this also bounds a single chunk.</summary>
        internal int PageSize => (int)allocator.GetPageSize();

        /// <summary>Number of pages in the circular buffer.</summary>
        internal int PageCount => allocator.MaxAllocatedPageCount;

        /// <summary>Lowest address still live; bytes below this have been truncated.</summary>
        internal long BeginAddress => allocator.BeginAddress;

        /// <summary>Next address to be allocated.</summary>
        internal long TailAddress => allocator.GetTailAddress();

        /// <summary>Lowest address still resident in memory.</summary>
        internal long HeadAddress => allocator.HeadAddress;

        /// <summary>All bytes below this address are on the device.</summary>
        internal long FlushedUntilAddress => allocator.FlushedUntilAddress;

        /// <summary>The page containing <paramref name="logicalAddress"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetPage(long logicalAddress) => allocator.GetPage(logicalAddress);

        /// <summary>The offset of <paramref name="logicalAddress"/> within its page.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetOffsetOnPage(long logicalAddress) => allocator.GetOffsetOnPage(logicalAddress);

        /// <summary>Bytes remaining on the page containing <paramref name="logicalAddress"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int RemainingOnPage(long logicalAddress) => PageSize - (int)GetOffsetOnPage(logicalAddress);
        #endregion

        #region Scope
        /// <summary>
        /// Enter an allocate-and-fill scope, acquiring epoch protection if this thread does not already hold it on this log's epoch.
        /// </summary>
        /// <returns>Whether this call acquired protection, to be passed to <see cref="EndScope"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool BeginScope() => epoch.ResumeIfNotProtected();

        /// <summary>Leave a scope entered by <see cref="BeginScope"/>.</summary>
        /// <param name="scopeAcquiredEpoch">The value returned by the matching <see cref="BeginScope"/>.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EndScope(bool scopeAcquiredEpoch)
        {
            if (scopeAcquiredEpoch)
                epoch.Suspend();
        }
        #endregion

        #region Allocation
        /// <summary>
        /// Allocate <paramref name="numBytes"/> contiguous bytes, waiting for a flush if the page ring is full.
        /// </summary>
        /// <param name="numBytes">Bytes to allocate; must not exceed <see cref="PageSize"/>.</param>
        /// <returns>The logical address of the allocation.</returns>
        /// <remarks>Must be called inside a <see cref="BeginScope"/> scope.</remarks>
        internal long Allocate(int numBytes)
        {
            var logicalAddress = AllocateCore(numBytes, minPartialBytes: 0, out var allocatedLength);
            Debug.Assert(allocatedLength == numBytes, $"Full allocation returned {allocatedLength} of {numBytes} requested bytes");
            return logicalAddress;
        }

        /// <summary>
        /// Allocate up to <paramref name="numBytes"/> contiguous bytes, waiting for a flush if the page ring is full.
        /// </summary>
        /// <param name="numBytes">Bytes wanted; must not exceed <see cref="PageSize"/>.</param>
        /// <param name="minPartialBytes">When nonzero, allow the allocation to be satisfied by the remainder of the current page,
        ///     provided both that remainder and what is left over are at least this many bytes. The caller fills what it got and
        ///     allocates the remainder separately.</param>
        /// <param name="allocatedLength">Bytes actually allocated, which is <paramref name="numBytes"/> unless the allocation was split.</param>
        /// <returns>The logical address of the allocation.</returns>
        /// <remarks>Must be called inside a <see cref="BeginScope"/> scope.</remarks>
        internal long Allocate(int numBytes, int minPartialBytes, out int allocatedLength)
            => AllocateCore(numBytes, minPartialBytes, out allocatedLength);

        private long AllocateCore(int numBytes, int minPartialBytes, out int allocatedLength)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), $"{nameof(Allocate)} must be called inside a {nameof(BeginScope)} scope");
            if (numBytes <= 0 || numBytes > PageSize)
                ThrowAllocationSize(numBytes);

            // Check before allocating as well as after. The allocator advances its tail before returning an address, so a ceiling
            // violation detected only afterwards leaves the tail permanently past the ceiling -- which a later checkpoint would then
            // record as the object-log tail. The post-allocation check below remains authoritative, because this one races with
            // concurrent allocators; this one keeps the common case from corrupting the tail on its way to failing.
            if (TailAddress + numBytes > MaxExclusiveAddress)
                ThrowAddressCeilingExceeded(TailAddress, numBytes);

            while (true)
            {
                var localFlushEvent = allocator.flushEvent;
                if (allocator.TryAllocateRetryNow(numBytes, minPartialBytes, out var logicalAddress, out var length))
                {
                    allocatedLength = (int)length;
                    CheckAddressCeiling(logicalAddress, allocatedLength);
                    return logicalAddress;
                }

                // RETRY_NOW (negative) is handled inside TryAllocateRetryNow, so this is RETRY_LATER: the page we need is still
                // being flushed. Drop the epoch so the flush and page close can make progress, then retry.
                Debug.Assert(logicalAddress == 0, $"Expected RETRY_LATER (0) but got {logicalAddress}");

                // A failed flush never frees the page this retry needs, and a failure also Set()s flushEvent and replaces it -- so
                // without this check the wait below returns, the loop captures the fresh unsignaled event, and parks forever.
                // Checked before the wait as well as after, because the failure may already have happened and swapped the event.
                // TryAllocateRetryNow only reaches ThrowIfFlushFailedForAllocation on its RETRY_NOW path, never on RETRY_LATER.
                allocator.ThrowIfFlushFailedForAllocation();

                epoch.Suspend();
                try
                {
                    localFlushEvent.Wait();
                }
                finally
                {
                    epoch.Resume();
                }

                // Disposal permanently signals flushEvent, so without this the wait above stops blocking and this loop spins on an
                // allocation that no flush can satisfy.
                allocator.ThrowIfDisposed();
                allocator.ThrowIfFlushFailedForAllocation();
            }
        }

        /// <summary>The physical address of <paramref name="logicalAddress"/>, valid only while the current scope holds the epoch and
        /// only until the next <see cref="Allocate(int)"/> on this thread.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetPhysicalAddress(long logicalAddress)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), $"{nameof(GetPhysicalAddress)} must be called inside a {nameof(BeginScope)} scope");
            return (byte*)allocator.GetPhysicalAddress(logicalAddress);
        }

        /// <summary>A writable span over an allocation, valid under the same conditions as <see cref="GetPhysicalAddress"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Span<byte> AsSpan(long logicalAddress, int length) => new(GetPhysicalAddress(logicalAddress), length);

        /// <summary>The lower of the position-word and page-number address ceilings for a log of this page size and page-ring size; see
        /// <see cref="MaxExclusiveAddress"/>.</summary>
        /// <param name="pageSizeBits">Page size in bits.</param>
        /// <param name="pageCount">Pages in the circular buffer, which bounds the page-index arithmetic needing headroom.</param>
        internal static long ComputeMaxExclusiveAddress(int pageSizeBits, int pageCount)
        {
            // Headroom covers the largest constant the allocator adds to a page number while deciding reuse: its BufferSize (the
            // power-of-two page-ring size), plus slack for the pageIndex + 1 lookahead.
            var headroomPages = (long)Utility.NextPowerOf2(pageCount) + 4;
            var maxExclusivePage = int.MaxValue - headroomPages;
            return Math.Min(maxExclusivePage << pageSizeBits, (long)ObjectLogPositionInfo.AddressMask);
        }

        private void CheckAddressCeiling(long logicalAddress, int allocatedLength)
        {
            if (logicalAddress + allocatedLength > MaxExclusiveAddress)
                ThrowAddressCeilingExceeded(logicalAddress, allocatedLength);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowAllocationSize(int numBytes)
            => throw new TsavoriteException($"Object log allocation of {numBytes} bytes must be greater than zero and no more than the page size ({PageSize})");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowAddressCeilingExceeded(long logicalAddress, int allocatedLength)
            => throw new TsavoriteException(
                $"Object log address space exhausted: an allocation ending at {logicalAddress + allocatedLength:N0} exceeds the"
                + $" object-log address ceiling of {MaxExclusiveAddress:N0} bytes. The address space is monotonic over the log's"
                + " lifetime, counting every byte ever written rather than bytes live, so this is reached only after sustained writes"
                + " over many years. Take a checkpoint to start a fresh object log.");
        #endregion

        #region Reading
        /// <summary>
        /// Read <paramref name="numBytes"/> bytes at <paramref name="logicalAddress"/> into <paramref name="destination"/>, from memory
        /// if the address is still resident and from the device otherwise.
        /// </summary>
        /// <remarks>Synchronous, and therefore not the path a multi-chunk read should take: the directory lets a reader issue all of an
        /// object's reads up front, which is the whole point of recording chunk addresses.</remarks>
        internal void Read(long logicalAddress, int numBytes, Span<byte> destination)
        {
            // Validate, choose the path, and submit all under one epoch acquisition. Validation reads BeginAddress and TailAddress,
            // and the device read resolves a segment handle, so letting protection lapse between any of those steps would allow a
            // concurrent Truncate to retire the segment the read is about to resolve.
            var scopeAcquiredEpoch = BeginScope();
            SectorAlignedMemory memory = null;
            ReadContext context = null;
            var validOffset = 0;
            try
            {
                ValidateReadRange(logicalAddress, numBytes, destination.Length);

                // An address at or above HeadAddress cannot be evicted while this scope holds the epoch, so the copy is safe.
                if (logicalAddress >= allocator.HeadAddress)
                {
                    new ReadOnlySpan<byte>(GetPhysicalAddress(logicalAddress), numBytes).CopyTo(destination);
                    return;
                }

                // Below HeadAddress, and HeadAddress never passes FlushedUntilAddress, so these bytes are on the device.
                memory = SubmitDeviceRead(logicalAddress, numBytes, out context, out validOffset);
            }
            finally
            {
                // Released before awaiting completion so a blocking wait cannot stall the drain list. The buffer and its in-flight
                // read are owned by this call, not by the page ring, so they do not depend on continued protection.
                EndScope(scopeAcquiredEpoch);
            }

            try
            {
                context.Completed.Wait();
                CompleteDeviceRead(logicalAddress, numBytes, context, memory, validOffset, destination);
            }
            finally
            {
                memory.Return();
                context.Completed.Dispose();
            }
        }

        private SectorAlignedMemory SubmitDeviceRead(long logicalAddress, int numBytes, out ReadContext context, out int validOffset)
        {
            allocator.ComputeAlignedReadExtent(logicalAddress, numBytes, out var alignedReadStart, out var alignedReadLength, out validOffset);

            var memory = allocator.bufferPool.Get((int)alignedReadLength, clearOnReturn: false);
            context = new ReadContext(alignedReadLength);
            try
            {
                device.ReadAsync(alignedReadStart, (IntPtr)memory.aligned_pointer, alignedReadLength, ReadCallback, context);
            }
            catch
            {
                memory.Return();
                throw;
            }
            return memory;
        }

        private void CompleteDeviceRead(long logicalAddress, int numBytes, ReadContext context, SectorAlignedMemory memory, int validOffset, Span<byte> destination)
        {
            if (context.ErrorCode != 0)
                throw new TsavoriteException($"Object log read of {numBytes} bytes at address {logicalAddress} failed with error code {context.ErrorCode}", context.Exception);

            // A device may report success having transferred fewer bytes than asked for -- a truncated or externally altered file does
            // exactly this. The buffer is rented without clearing, so copying the full request on a short read would hand back whatever
            // the previous tenant left in the shortfall. Treat insufficient coverage as a failure, as the object allocator's own read
            // completion does.
            if (context.BytesTransferred < (uint)(validOffset + numBytes))
                throw new TsavoriteException(
                    $"Object log read of {numBytes} bytes at address {logicalAddress} returned only {context.BytesTransferred} of"
                    + $" {context.RequestedBytes} requested bytes; the device reported success but did not cover the requested range");

            new ReadOnlySpan<byte>(memory.aligned_pointer + validOffset, numBytes).CopyTo(destination);
        }

        internal sealed class ReadContext(uint requestedBytes)
        {
            internal readonly ManualResetEventSlim Completed = new(initialState: false);
            internal readonly uint RequestedBytes = requestedBytes;
            internal uint BytesTransferred;
            internal uint ErrorCode;
            internal Exception Exception;
        }

        private static void ReadCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var readContext = (ReadContext)context;
            readContext.ErrorCode = errorCode;
            readContext.BytesTransferred = numBytes;
            readContext.Exception = ioException;
            readContext.Completed.Set();
        }

        private void ValidateReadRange(long logicalAddress, int numBytes, int destinationLength)
        {
            if (numBytes <= 0 || numBytes > destinationLength)
                throw new TsavoriteException($"Object log read length {numBytes} must be greater than zero and no more than the destination length {destinationLength}");
            if (logicalAddress < allocator.BeginAddress)
                throw new TsavoriteException($"Object log read at address {logicalAddress} is below BeginAddress {allocator.BeginAddress}");
            if (logicalAddress + numBytes > TailAddress)
                throw new TsavoriteException($"Object log read of {numBytes} bytes at address {logicalAddress} extends past TailAddress {TailAddress}");

            // Reads translate to one device extent, which cannot cross a page because pages are written independently. The format
            // guarantees this by never letting an entry span a page.
            if (GetPage(logicalAddress) != GetPage(logicalAddress + numBytes - 1))
                throw new TsavoriteException($"Object log read of {numBytes} bytes at address {logicalAddress} spans a page boundary");
        }

        /// <summary>
        /// Start reading [<paramref name="logicalAddress"/>, <paramref name="logicalAddress"/> + <paramref name="numBytes"/>) into a buffer
        /// the returned read owns, for a reader that knows ahead of time which bytes it will want.
        /// </summary>
        /// <remarks>
        /// A range below HeadAddress is read from the device and stops at HeadAddress, since the bytes above it may not be on the device
        /// yet; a range at or above HeadAddress is copied from memory now. So the read may cover less than was asked for: see
        /// <see cref="RangeRead.EndAddress"/>. The caller must dispose the read, which waits for its IO.
        /// </remarks>
        /// <returns>The read, or null if the range is not within [BeginAddress, TailAddress) and one page, so nothing was read.</returns>
        internal RangeRead TryBeginRangeRead(long logicalAddress, int numBytes)
        {
            // One epoch acquisition covers validation, the choice of path, and the submission, as for Read.
            var scopeAcquiredEpoch = BeginScope();
            try
            {
                if (numBytes <= 0 || logicalAddress < allocator.BeginAddress || logicalAddress + numBytes > TailAddress
                        || GetPage(logicalAddress) != GetPage(logicalAddress + numBytes - 1))
                    return null;

                var headAddress = allocator.HeadAddress;
                if (logicalAddress >= headAddress)
                {
                    // An address at or above HeadAddress cannot be evicted while this scope holds the epoch, so the copy is safe.
                    var copy = allocator.bufferPool.Get(numBytes, clearOnReturn: false);
                    new ReadOnlySpan<byte>(GetPhysicalAddress(logicalAddress), numBytes).CopyTo(new Span<byte>(copy.aligned_pointer, numBytes));
                    return new RangeRead(logicalAddress, numBytes, copy, validOffset: 0, context: null);
                }

                if (logicalAddress + numBytes > headAddress)
                    numBytes = (int)(headAddress - logicalAddress);
                var memory = SubmitDeviceRead(logicalAddress, numBytes, out var context, out var validOffset);
                return new RangeRead(logicalAddress, numBytes, memory, validOffset, context);
            }
            finally
            {
                EndScope(scopeAcquiredEpoch);
            }
        }

        /// <summary>A range of the log read into a buffer the read owns; see <see cref="TryBeginRangeRead"/>.</summary>
        /// <remarks>Used by one thread at a time.</remarks>
        internal sealed class RangeRead : IDisposable
        {
            /// <summary>The first address read.</summary>
            internal readonly long StartAddress;

            /// <summary>One past the last address read.</summary>
            internal readonly long EndAddress;

            readonly SectorAlignedMemory memory;
            readonly int validOffset;

            /// <summary>The device read, or null when the range was copied from memory.</summary>
            readonly ReadContext context;

            /// <summary>Whether the device read is known to have covered the range; null until its completion is observed.</summary>
            bool? succeeded;

            internal RangeRead(long startAddress, int length, SectorAlignedMemory memory, int validOffset, ReadContext context)
            {
                StartAddress = startAddress;
                EndAddress = startAddress + length;
                this.memory = memory;
                this.validOffset = validOffset;
                this.context = context;
            }

            /// <summary>Copy the bytes at <paramref name="logicalAddress"/>, which must lie within this read, to <paramref name="destination"/>,
            /// waiting for the device read if it is still in flight.</summary>
            /// <returns>False if the device read failed or fell short, so the bytes are not available here.</returns>
            internal bool TryCopyTo(long logicalAddress, Span<byte> destination)
            {
                Debug.Assert(logicalAddress >= StartAddress && logicalAddress + destination.Length <= EndAddress, "The bytes must lie within the read");
                if (context is not null)
                {
                    if (succeeded is null)
                    {
                        context.Completed.Wait();
                        succeeded = context.ErrorCode == 0 && context.BytesTransferred >= (uint)(validOffset + EndAddress - StartAddress);
                    }
                    if (!succeeded.Value)
                        return false;
                }
                new ReadOnlySpan<byte>(memory.aligned_pointer + validOffset + (logicalAddress - StartAddress), destination.Length).CopyTo(destination);
                return true;
            }

            /// <summary>Wait for the device read, if any, and return the buffer.</summary>
            public void Dispose()
            {
                // Never return a buffer a device read may still write into.
                context?.Completed.Wait();
                memory.Return();
                context?.Completed.Dispose();
            }
        }
        #endregion

        #region Durability and lifecycle
        /// <summary>
        /// Make everything written so far durable.
        /// </summary>
        /// <param name="wait">Whether to wait for the flush to reach the device.</param>
        /// <returns>The address through which the flush was requested.</returns>
        internal long FlushTail(bool wait)
        {
            var tailAddress = TailAddress;
            allocator.ShiftReadOnlyAddressWithWait(tailAddress, wait: false);
            if (wait)
                WaitForFlush(tailAddress);
            return tailAddress;
        }

        /// <summary>Wait until <paramref name="untilAddress"/> is on the device. The caller is responsible for having requested a flush
        /// that covers it (see <see cref="FlushTail"/>).</summary>
        /// <remarks>Safe to call while holding the epoch: the flush is issued by work queued on the epoch's drain list, so a holder
        /// that merely spun would block the very progress it is waiting for. A caller that does not hold the epoch spins briefly and
        /// then blocks on the allocator's flush event rather than yielding in a loop, since a page write can take milliseconds.</remarks>
        internal void WaitForFlush(long untilAddress)
        {
            if (epoch.ThisInstanceProtected())
            {
                while (!disposed && FlushedUntilAddress < untilAddress)
                {
                    allocator.ThrowIfFlushFailedBelow(untilAddress);
                    epoch.ProtectAndDrain();
                }
                return;
            }

            var spins = 0;
            while (true)
            {
                // Capture the event before testing, so a flush that completes between the test and the wait signals this event.
                var localFlushEvent = allocator.flushEvent;
                if (disposed || allocator.IsDisposed || FlushedUntilAddress >= untilAddress)
                    return;

                // A failed flush never advances FlushedUntilAddress, and its signal may predate the capture above, leaving nothing to
                // signal this generation; check before waiting so the wait below cannot be endless.
                allocator.ThrowIfFlushFailedBelow(untilAddress);
                if (++spins < Constants.kFlushSpinCount)
                {
                    _ = Thread.Yield();
                    continue;
                }
                localFlushEvent.Wait();
            }
        }

        /// <summary>
        /// Invoke <paramref name="callback"/> once <see cref="FlushedUntilAddress"/> reaches <paramref name="untilAddress"/>, or with a nonzero
        /// error code if a flush below it fails or the log is disposed first.
        /// </summary>
        /// <param name="untilAddress">The address to wait for. The caller is responsible for having requested a flush that covers it (see
        /// <see cref="FlushTail"/>).</param>
        /// <param name="callback">Invoked exactly once, with <c>numBytes</c> zero: on this thread if the outcome is already known, otherwise on the
        /// thread that completes the flush. It must not throw.</param>
        /// <param name="context">Passed to <paramref name="callback"/>.</param>
        /// <remarks>This lets a main-log page complete once its object bytes are durable without a thread blocking on the object-log flush.</remarks>
        internal void OnFlushed(long untilAddress, DeviceIOCompletionCallback callback, object context)
        {
            uint errorCode;
            Exception exception;
            lock (flushWaitersLock)
            {
                if (FlushedUntilAddress >= untilAddress)
                    (errorCode, exception) = (0, null);
                else if (disposed)
                    (errorCode, exception) = (uint.MaxValue, new ObjectDisposedException(nameof(ObjectLog)));
                else if (flushFailure is CommitInfo failure && failure.FromAddress < untilAddress)
                    (errorCode, exception) = (failure.ErrorCode, CreateFlushFailureException(failure));
                else
                {
                    flushWaiters.Add(new FlushWaiter(untilAddress, callback, context));
                    return;
                }
            }
            InvokeFlushWaiter(new FlushWaiter(untilAddress, callback, context), errorCode, exception);
        }

        /// <summary>The allocator's flush callback: reports each advance of <see cref="FlushedUntilAddress"/>, and the earliest flush failure once
        /// every flush below it has completed.</summary>
        private void OnAllocatorFlush(CommitInfo info)
        {
            List<FlushWaiter> completed = null, failed = null;
            lock (flushWaitersLock)
            {
                if (info.ErrorCode != 0)
                    flushFailure ??= info;

                // Read FlushedUntilAddress afresh: concurrent completions may report their advances out of order.
                var flushedUntilAddress = FlushedUntilAddress;
                for (var ii = flushWaiters.Count - 1; ii >= 0; ii--)
                {
                    var waiter = flushWaiters[ii];
                    if (waiter.UntilAddress <= flushedUntilAddress)
                        (completed ??= []).Add(waiter);
                    else if (flushFailure is CommitInfo failure && failure.FromAddress < waiter.UntilAddress)
                        (failed ??= []).Add(waiter);
                    else
                        continue;
                    flushWaiters.RemoveAt(ii);
                }
            }

            if (completed is not null)
            {
                for (var ii = completed.Count - 1; ii >= 0; ii--)
                    InvokeFlushWaiter(completed[ii], errorCode: 0, exception: null);
            }
            if (failed is not null)
            {
                var exception = CreateFlushFailureException(flushFailure.Value);
                for (var ii = failed.Count - 1; ii >= 0; ii--)
                    InvokeFlushWaiter(failed[ii], flushFailure.Value.ErrorCode, exception);
            }
        }

        private void InvokeFlushWaiter(in FlushWaiter waiter, uint errorCode, Exception exception)
        {
            try
            {
                waiter.Callback(errorCode, numBytes: 0, waiter.Context, exception);
            }
            catch (Exception ex)
            {
                // The callback runs on a flush-completion path, which an exception would tear down.
                logger?.LogError(ex, "Object log flush callback for address {untilAddress} threw", waiter.UntilAddress);
            }
        }

        private static TsavoriteException CreateFlushFailureException(CommitInfo failure)
            => new($"Object log flush [{failure.FromAddress}, {failure.UntilAddress}) failed with error code {failure.ErrorCode}", failure.Exception);

        /// <summary>Retire everything below <paramref name="newBeginAddress"/>, deleting device segments that fall entirely below it.</summary>
        /// <remarks>Acquires epoch protection if the caller does not hold it: the shift publishes addresses and then bumps the epoch to
        /// run the page-close and device-truncate work behind that barrier.</remarks>
        internal void Truncate(long newBeginAddress)
        {
            var scopeAcquiredEpoch = BeginScope();
            try
            {
                allocator.ShiftBeginAddress(newBeginAddress, truncateLog: true);
            }
            finally
            {
                EndScope(scopeAcquiredEpoch);
            }
        }

        /// <summary>
        /// Position the log at a tail recovered from checkpoint metadata.
        /// </summary>
        /// <param name="beginAddress">Recovered begin address.</param>
        /// <param name="tailAddress">Recovered tail address; appends resume here.</param>
        /// <remarks>
        /// Only the page containing <paramref name="tailAddress"/> is loaded, and only when the tail falls mid-page. **That load is
        /// required for correctness, not for read performance.** A flush writes whole sectors, so the first flush after recovery
        /// rewrites the sector the recovered tail sits in; if the page buffer were left zeroed below the tail, that flush would write
        /// zeros over the recovered bytes sharing the sector and silently destroy them. Everything below the tail page stays on the
        /// device and is read on demand, which is the point of giving the object log little memory.
        /// </remarks>
        internal void RestoreTail(long beginAddress, long tailAddress)
        {
            if (beginAddress < LogAddress.FirstValidAddress)
                throw new TsavoriteException($"Object log begin address {beginAddress} is below the first valid address {LogAddress.FirstValidAddress}");
            if (tailAddress < beginAddress)
                throw new TsavoriteException($"Object log tail address {tailAddress} is below its begin address {beginAddress}");

            // Load from the start of the tail's page, so a partially filled tail page is read back before anything appends to it.
            var headAddress = tailAddress - allocator.GetOffsetOnPage(tailAddress);
            if (headAddress < beginAddress)
                headAddress = beginAddress;

            allocator.RestoreHybridLog(beginAddress, headAddress, fromAddress: tailAddress, untilAddress: tailAddress);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            List<FlushWaiter> abandoned;
            lock (flushWaitersLock)
            {
                if (disposed)
                    return;
                disposed = true;
                abandoned = [.. flushWaiters];
                flushWaiters.Clear();
            }

            // A waiter's flush can no longer complete, so fail it rather than leave its caller waiting.
            if (abandoned.Count > 0)
            {
                var exception = new ObjectDisposedException(nameof(ObjectLog));
                foreach (var waiter in abandoned)
                    InvokeFlushWaiter(waiter, uint.MaxValue, exception);
            }

            allocator.Dispose();
            if (isEpochOwned)
                epoch.Dispose();
        }
        #endregion

        /// <inheritdoc/>
        public override string ToString()
            => $"Begin {BeginAddress}; Head {HeadAddress}; FlushedUntil {FlushedUntilAddress}; Tail {TailAddress}; PageSize {PageSize:N0}";
    }
}