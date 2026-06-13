// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    struct MultiLevelPageArray
    {
        // TODO: Make MLPA config numbers internally configurable (e.g. smaller log pages need less overhead). Should be able to do this internally
        //      and not expose another set of public config options.
        internal const int InitialBookSizeBits = 2;
        internal const int PrimaryClearRetainedPageSizeBits = InitialBookSizeBits << 1;
        internal const int FreeListClearRetainedPageSizeBits = InitialBookSizeBits;
        internal const int PageSizeBits = 10;

        internal const int InitialBookSize = 1 << InitialBookSizeBits;
        internal const int PageSize = 1 << PageSizeBits;
        internal const int BlockIndexMask = (1 << PageSizeBits) - 1;
    }

    /// <summary>
    /// This creates a 3-d array of block vectors. This can be envisioned as a book, where the first two dimensions are infrastructure, and the third is where
    /// the user-visible allocations (blocks) are created.
    /// <list type="bullet">
    ///     <item>The first dimension is the "book", which is a collection of "pages".</item>
    ///     <item>The second dimension is the "pages", each a fixed-size collection of blocks.</item>
    ///     <item>The third dimension is the actual blocks of data which are returned to the user.</item>
    /// </list>
    /// This structure is chosen so that only the "book" is grown; individual pages are allocated as a fixed size. This means that
    /// getting and clearing items in the page does not have to take a latch to prevent a lost update as the array is grown, as
    /// would be necessary if there was only a single level of infrastructure (i.e. a growable page).
    /// <para>
    /// Concurrency model: <see cref="tail"/> is a packed <see cref="PageOffset"/> (<see cref="PageOffset.Page"/>/<see cref="PageOffset.Offset"/>
    /// within a single 64-bit field, where <see cref="PageOffset.Offset"/> is reused to mean "block index within the page"). A single
    /// <see cref="Interlocked.Add(ref long, long)"/> on <see cref="PageOffset.PageAndOffset"/> both claims the slot and identifies the unique
    /// thread that owns the next page-turn: the natural owner is the thread whose post-Add Offset is exactly <c>PageSize + 1</c>. That thread
    /// drives <see cref="AddPage"/> and then publishes the new (Page, Offset=1) with a single <see cref="Interlocked.Exchange(ref long, long)"/>
    /// that simultaneously wipes any concurrent "loser" Adds (post-Offset &gt; PageSize + 1). On <see cref="AddPage"/> failure the owner rewinds
    /// the same way, so the next Add becomes the new natural owner and retries; no slot ids are ever lost or duplicated.
    /// </para>
    /// </summary>
    internal class MultiLevelPageArray<TElement>
    {
        internal TElement[][] book;

        /// <summary>The packed (Page, Offset) tail. <see cref="PageOffset.Offset"/> here represents the block index within the page rather
        /// than a byte offset. <see cref="PageOffset.PageAndOffset"/> == 0 is the "uninitialized" sentinel; the live initial value (set by
        /// <see cref="EnsureInitialized"/>) is <c>Page = -1, Offset = PageSize</c> so that the very first <see cref="Interlocked.Add(ref long, long)"/>
        /// of 1 yields <c>Page = -1, Offset = PageSize + 1</c>, which is recognized as the natural owner of page 0.</summary>
        internal PageOffset tail;

        public bool IsInitialized => book is not null;

        /// <summary>The number of ids that have been claimed (i.e. the next id that would be returned by <see cref="Allocate"/>).</summary>
        /// <remarks>Snapshots <see cref="tail"/> via <see cref="Volatile"/>.Read so cross-thread reads do not tear on 32-bit. Clamps the
        /// offset to <see cref="MultiLevelPageArray.PageSize"/> to avoid over-counting during the brief cold window where an owner has
        /// observed its claim post-Add but has not yet published; this transiently under-counts by at most one until the publish lands.</remarks>
        public int Count
        {
            get
            {
                PageOffset snap = default;
                snap.PageAndOffset = Volatile.Read(ref tail.PageAndOffset);
                if (snap.PageAndOffset == 0)
                    return 0;
                var offset = snap.Offset > MultiLevelPageArray.PageSize ? MultiLevelPageArray.PageSize : snap.Offset;
                return (snap.Page * MultiLevelPageArray.PageSize) + offset;
            }
        }

        /// <summary>
        /// Allocate a new id. Spins on <see cref="TryAllocate(out int)"/> while it returns false (a transient "cold window" or "loser of the
        /// page-turn race"). May throw if <see cref="AddPage"/> throws (typically OOM); on throw <see cref="tail"/> is rewound atomically so a
        /// subsequent <see cref="Allocate"/> can retry without losing or duplicating any slot ids.
        /// </summary>
        public int Allocate()
        {
            int id;
            while (!TryAllocate(out id))
                _ = Thread.Yield();
            return id;
        }

        /// <summary>
        /// Attempt a single allocation step. Returns <see langword="true"/> with a valid <paramref name="id"/> when our claim landed on the
        /// fast path, or when we were the unique natural owner of a new page and successfully drove <see cref="AddPage"/>. Returns
        /// <see langword="false"/> in two transient cases that the caller should handle by yielding and retrying: (a) the cold window in
        /// which an owner is mid-<see cref="AddPage"/> and our snapshot would burn an Offset slot the owner is about to wipe, and (b) we
        /// were a "loser" of the page-turn race -- our Add bumped the Offset past <c>PageSize + 1</c> and will be wiped by the owner's
        /// publish-Exchange. Throws on <see cref="AddPage"/> failure (after rewinding <see cref="tail"/>).
        /// </summary>
        public bool TryAllocate(out int id)
        {
            // First-call cold path: lazily initialize 'book' and 'tail'. After the first successful TryAllocate on this instance (until Clear()
            // resets us), tail.PageAndOffset stays non-zero and we skip this entirely.
            if (tail.PageAndOffset == 0)
                EnsureInitialized();

            // Value-type snapshot. On 64-bit a single load of the long is atomic; on 32-bit it may tear, in which case a "weird" Offset will
            // simply cause us to fail the cold-window check or land in the loser branch and the caller will retry. We do not need Volatile
            // here because the Interlocked.Add below is the synchronization point that establishes our claim. 'default' initialization is
            // required so the compiler treats the overlapping Offset/Page fields as definitely assigned alongside PageAndOffset.
            PageOffset local = default;
            local.PageAndOffset = tail.PageAndOffset;

            // Cold window: an owner is in CompleteAsPageOwner with the next page mid-allocation. If we Add now we just burn an Offset slot
            // that will be wiped by the owner's publish-Exchange. Bail without incrementing so we don't add to the loser pile-up.
            if (local.Offset > MultiLevelPageArray.PageSize)
            {
                id = 0;
                return false;
            }

            // Atomic claim. The post-image's Offset tells us our role:
            //   <= PageSize           : fast path -- we claimed block (Offset - 1) of page Page.
            //   == PageSize + 1       : unique natural owner of the next page -- drive AddPage + publish.
            //   >  PageSize + 1       : loser; the owner's publish-Exchange will overwrite our Add. Bail; caller yields and retries.
            local.PageAndOffset = Interlocked.Add(ref tail.PageAndOffset, 1);

            if (local.Offset <= MultiLevelPageArray.PageSize)
            {
                id = (local.Page << MultiLevelPageArray.PageSizeBits) | (local.Offset - 1);
                return true;
            }

            if (local.Offset == MultiLevelPageArray.PageSize + 1)
            {
                id = CompleteAsPageOwner(local);
                return true;
            }

            // Loser of the page-turn race. Do not try to undo our Add; the owner's Exchange will overwrite it.
            id = 0;
            return false;
        }

        /// <summary>
        /// Cold initial-state initializer for <see cref="TryAllocate(out int)"/>. Runs once per instance (or once after each <see cref="Clear(int)"/>).
        /// Hoisted into its own NoInlining method so the hot path stays small enough to inline cleanly into callers.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EnsureInitialized()
        {
            // The live initial value of tail is (Page = -1, Offset = PageSize); the first Add(1) yields (Page = -1, Offset = PageSize + 1)
            // which is recognized by TryAllocate as the natural owner of page 0. tail.PageAndOffset == 0 is reserved as the "uninitialized"
            // sentinel and never appears as a live value. 'default' initialization first so Offset and Page are definitely assigned for the
            // compiler before we then write through their overlapping fields.
            PageOffset initial = default;
            initial.Page = -1;
            initial.Offset = MultiLevelPageArray.PageSize;

            while (tail.PageAndOffset == 0)
            {
                // The book may be non-null due to Clear() (e.g. when we wrap around in the log to page 0) or to the newBook allocation below.
                // If the book is not null but tail is still the uninitialized sentinel, install the live initial value once via CAS.
                if (book is not null)
                {
                    _ = Interlocked.CompareExchange(ref tail.PageAndOffset, initial.PageAndOffset, 0);
                    continue;
                }

                // Allocate the book as a two-step process so we don't overwrite another thread's book allocation. Because we can't set both
                // the book and tail in a single atomic operation, we set only the book, then loop back up to detect the non-null book and
                // install the live initial tail; otherwise multiple threads could install tail at two distinct locations.
                var newBook = new TElement[MultiLevelPageArray.InitialBookSize][];
                if (Interlocked.CompareExchange(ref book, newBook, null) == null)
                    continue;
                _ = Thread.Yield();
            }
        }

        /// <summary>
        /// Slow path of <see cref="TryAllocate(out int)"/> for the unique thread whose post-Add Offset is <c>PageSize + 1</c>. Drives
        /// <see cref="AddPage"/> for the next page and publishes the new <see cref="tail"/> via a single
        /// <see cref="Interlocked.Exchange(ref long, long)"/> that simultaneously wipes any concurrent loser Adds. On <see cref="AddPage"/>
        /// failure rewinds <see cref="tail"/> the same way so the next Add becomes the new natural owner.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private int CompleteAsPageOwner(PageOffset claim)
        {
            // claim.Page is the previous page; we are taking block 0 of (claim.Page + 1).
            var newPageIdx = claim.Page + 1;
            try
            {
                AddPage(newPageIdx);
            }
            catch
            {
                // Rewind atomically to (claim.Page, PageSize) so the next thread's Add yields (claim.Page, PageSize + 1) and becomes the new
                // natural owner of newPageIdx. Exchange (not CAS) because losers' concurrent Adds may have already bumped Offset past
                // PageSize + 1; the Exchange wipes those as well.
                PageOffset rewind = default;
                rewind.Page = claim.Page;
                rewind.Offset = MultiLevelPageArray.PageSize;
                _ = Interlocked.Exchange(ref tail.PageAndOffset, rewind.PageAndOffset);
                throw;
            }

            // Publish: I just took block 0 of newPageIdx. Exchange wipes any losers' Adds in one shot.
            PageOffset publish = default;
            publish.Page = newPageIdx;
            publish.Offset = 1;
            _ = Interlocked.Exchange(ref tail.PageAndOffset, publish.PageAndOffset);
            return newPageIdx << MultiLevelPageArray.PageSizeBits;
        }

        /// <summary>
        /// Add a new page. Only the natural owner of the next page-turn ever calls this, so no per-page CAS coordination is needed: the book
        /// grow and page allocation are simple straight-line code. Idempotent for pages already populated by a previous generation that
        /// <see cref="Clear(int)"/> retained. NoInlining because the call site is on the cold path of <see cref="TryAllocate(out int)"/>;
        /// keeping it out of line lets the hot path stay compact.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AddPage(int newPageIdx)
        {
            // Single-owner invariant: only the natural owner of the page-turn is here, and natural owners are serialized by the page-turn
            // discipline (a thread can only become the owner of newPageIdx after the previous page's owner has published). So newPageIdx can
            // never exceed book.Length by more than 1.
            Debug.Assert(newPageIdx <= book.Length, $"newPageIdx {newPageIdx} cannot exceed book.Length {book.Length} (single-owner discipline broken?)");

            // Test hook: in DEBUG builds, optionally throw before any state mutation so test code can deterministically exercise the rewind
            // path in CompleteAsPageOwner. Stripped from Release builds via [Conditional("DEBUG")].
            MaybeInjectAddPageFailure(newPageIdx);

            // Idempotent on Clear-retained pages: if the page is already populated (because the previous generation's Clear kept it), reuse it.
            if (newPageIdx < book.Length && book[newPageIdx] is not null)
                return;

            if (newPageIdx == book.Length)
            {
                var newBook = new TElement[book.Length * 2][];
                Array.Copy(book, newBook, book.Length);
                book = newBook;
            }

            book[newPageIdx] = new TElement[MultiLevelPageArray.PageSize];
        }

        /// <summary>Test-only hook for forcing <see cref="AddPage"/> failure. Stripped from Release builds via <see cref="ConditionalAttribute"/>.</summary>
        [Conditional("DEBUG")]
        private void MaybeInjectAddPageFailure(int newPageIdx)
        {
#if DEBUG
            var hook = testInjectAddPageFailure;
            if (hook is not null && hook(newPageIdx))
                throw new OutOfMemoryException($"Test-injected OOM on AddPage({newPageIdx})");
#endif
        }

#if DEBUG
        /// <summary>Test-only injection hook: if non-null and returns <see langword="true"/> for the given new page index,
        /// <see cref="AddPage"/> throws <see cref="OutOfMemoryException"/> before any state mutation. Field only exists in DEBUG builds.</summary>
        internal Func<int, bool> testInjectAddPageFailure;
#endif

        public TElement this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Get(index);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Set(index, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TElement Get(int index)
        {
            // Cache Count in a local so the assertion check and its message see the same value. (tail is monotonically non-decreasing in the
            // non-OOM path, but a concurrent Allocate on another thread can advance it between the two reads we would otherwise do, producing
            // a misleading assertion message such as "index 1 must be less than Count 7" when at the moment of the failing check Count was <= 1.)
            var countSnapshot = Count;
            Debug.Assert(index < countSnapshot, $"Get(): index {index} must be less than Count {countSnapshot}");
            var localBook = book;   // Temp copy as 'book' may be reallocated while we do this (but the page indexing remains unchanged and the page remains valid).

            var pageIndex = index >> MultiLevelPageArray.PageSizeBits;
            var blockIndex = index & MultiLevelPageArray.BlockIndexMask;
            Debug.Assert(localBook[pageIndex] is not null, $"index {index} out of range of pages {pageIndex}");
            return localBook[pageIndex][blockIndex];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int index, TElement element)
        {
            // Cache Count in a local; see comment in Get for rationale.
            var countSnapshot = Count;
            Debug.Assert(index < countSnapshot, $"Set(): index {index} must be less than Count {countSnapshot}");
            var localBook = book;   // Temp copy as 'book' may be reallocated while we do this (but the page indexing remains unchanged and the page remains valid).

            var pageIndex = index >> MultiLevelPageArray.PageSizeBits;
            var blockIndex = index & MultiLevelPageArray.BlockIndexMask;
            Debug.Assert(localBook[pageIndex] is not null, $"index {index} out of range of pages {pageIndex}");
            localBook[pageIndex][blockIndex] = element;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear(int retainedPageCount = 1 << MultiLevelPageArray.PrimaryClearRetainedPageSizeBits)
        {
            if (!IsInitialized)
                return;
            var count = Count;
            if (count == 0)
                return;

            // Count is the next id to return, so claimed ids span [0, count-1]. lastPageIndex is the page containing the last claimed id.
            var lastPageIndex = (count - 1) >> MultiLevelPageArray.PageSizeBits;
            for (var page = 0; page <= lastPageIndex; page++)
            {
                Array.Clear(book[page], 0, MultiLevelPageArray.PageSize);
                if (page > retainedPageCount)
                    book[page] = null;
            }

            tail.PageAndOffset = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear(Action<TElement> action, int retainedPageCount = 1 << MultiLevelPageArray.PrimaryClearRetainedPageSizeBits)
        {
            if (!IsInitialized)
                return;
            var count = Count;
            if (count == 0)
                return;

            // Count is the next id to return; claimed ids span [0, count-1]. lastPageIndex is the page containing the last claimed id;
            // lastBlockIndex is the block index of the last claimed id within that page.
            var lastPageIndex = (count - 1) >> MultiLevelPageArray.PageSizeBits;
            var lastBlockIndex = (count - 1) & MultiLevelPageArray.BlockIndexMask;
            for (var page = 0; page <= lastPageIndex; page++)
            {
                var maxBlock = page < lastPageIndex ? MultiLevelPageArray.PageSize : lastBlockIndex;
                for (var block = 0; block < maxBlock; block++)
                {
                    // Note: 'action' must check for null/default.
                    action(book[page][block]);
                    book[page][block] = default;
                }
                if (page > retainedPageCount)
                    book[page] = null;
            }
            tail.PageAndOffset = 0;
        }

        /// <inheritdoc/>
        public override string ToString() => $"Tail: [{tail}], Count: {Count}, IsInit: {IsInitialized}, book.Len: {(book is not null ? book.Length : "null")}";
    }
}