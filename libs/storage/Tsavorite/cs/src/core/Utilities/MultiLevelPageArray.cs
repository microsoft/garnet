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
        internal const int PrimaryClearRetainedChapterSizeBits = InitialBookSizeBits << 1;
        internal const int FreeListClearRetainedChapterSizeBits = InitialBookSizeBits;
        internal const int ChapterSizeBits = 10;

        internal const int InitialBookSize = 1 << InitialBookSizeBits;
        internal const int ChapterSize = 1 << ChapterSizeBits;
        internal const int PageIndexMask = (1 << ChapterSizeBits) - 1;
    }

    /// <summary>
    /// This creates a 3-d array of page vectors. This can be envisioned as a book, where the first two dimensions are infrastructure, and the third is where
    /// the user-visible allocations are created.
    /// <list type="bullet">
    ///     <item>The first dimension is the "book", which is a collection of "chapters".</item>
    ///     <item>The second dimension is the "chapters", which is a collection of pages.</item>
    ///     <item>The third dimension is the actual pages of data which are returned to the user</item>
    /// </list>
    /// This structure is chosen so that only the "book" is grown; individual chapters are allocated as a fixed size. This means that
    /// getting and clearing items in the chapter does not have to take a latch to prevent a lost update as the array is grown, as
    /// would be necessary if there was only a single level of infrastructure (i.e. a growable chapter).
    /// </summary>
    internal class MultiLevelPageArray<TElement>
    {
        internal TElement[][] book;

        /// <summary>Value of the tail before initialization; start at -1 so Allocate() sets it to 0</summary>
        private const int InitialTail = -1;

        /// <summary>The next index to be returned; <see cref="InitialTail"/> if we are not yet initialized.</summary>
        internal int tail = InitialTail;

        public bool IsInitialized => book is not null;

        public int Count => tail < 0 ? 0 : tail;   // InitialTail is -1, and tail is the next id to return

        public int Allocate()
        {
            // The first loop ensures the book is initialized. It may be repeated up to three times; if either of the CompareExchanges fails,
            // it will be because the desired condition was already set by another thread.
            while (tail == InitialTail)
            {
                // The book may be non-null due to Clear() (e.g. when we wrap around in the log to page 0) or to the newBook allocation below.
                // If the book is not null but the tail is InitialTail, we need to set tail to 0 *once* to start allocations.
                if (book is not null)
                {
                    _ = Interlocked.CompareExchange(ref tail, 0, InitialTail);
                    continue;
                }

                // Another thread may have allocated and set the book since the last test; if not, try to do so now
                if (book is null)
                {
                    // Allocate the book as a two-step process so we don't overwrite another thread's book allocation. Because we can't set both the
                    // book and tail in a single atomic operation, we set only the book, and loop back up to detect the non-null book and set tail
                    // instead of setting it here; otherwise, there is a race where multiple threads could set tail to 0 at the two locations.
                    var newBook = new TElement[MultiLevelPageArray.InitialBookSize][];
                    if (Interlocked.CompareExchange(ref book, newBook, null) == null)
                        continue;
                }
                _ = Thread.Yield();
            }

            while (true)
            {
                var originalTail = tail;
                var originalChapter = originalTail >> MultiLevelPageArray.ChapterSizeBits;
                if ((originalChapter >= book.Length || book[originalChapter] is null) && ((originalTail & MultiLevelPageArray.PageIndexMask) > 0))
                {
                    // Only the first-page return in a chapter can allocate the chapter and one or more other threads has already incremented tail
                    // into a new, not-yet-allocated chapter, which means the first one that did so owns the "latch" to allocate this chapter.
                    // Other threads should exit without incrementing tail; just wait for that owning thread to allocate the new chapter.
                    _ = Thread.Yield(); // TODO consider SpinWait.SpinOnce() with backoff
                    continue;
                }

                // If we are here, our first test indicated we did not need to allocate a new chapter (but that may have changed), or we are a candidate to
                // be the first to allocate it and thus own the new-chapter "latch". Increment tail and we'll return the prior-to-increment value once we have
                // done any needed allocation.
                var returnTail = Interlocked.Increment(ref tail) - 1;
                var newChapter = returnTail >> MultiLevelPageArray.ChapterSizeBits;
                Debug.Assert(newChapter >= originalChapter, $"newChapter {newChapter} must not be < originalChapter {originalChapter}");
                if (newChapter < book.Length && book[newChapter] is not null)
                    return returnTail;

                // Multiple threads might have seen the initial "first page in chapter" condition and incremented tail. We only want to stay if we're the
                // first; that is, if we are returning the first page in the new chapter. Others threads should exit and wait and the first thread will
                // "release the increment" by resetting tail after it's allocated the chapter.
                var newPage = returnTail & MultiLevelPageArray.PageIndexMask;
                if (newPage > 0)
                {
                    _ = Thread.Yield();
                    continue;
                }

                // We are allocating the first page on a new chapter so we "own the latch" on this newChapter, but it's (barely) possible that tail was incremented
                // so many times it went to a second page. Therefore only try to allocate newChapter if it is the first in the book or the previous chapter is allocated.
                if (newChapter > 0 && book[newChapter - 1] is null)
                {
                    _ = Thread.Yield();
                    continue;
                }

                // We still own the latch and are allocating the new chapter, and possibly need to grow the book. If this returns false, tail is reset to the first page
                // in the chapter; otherwise it is set to the second and we return the first. These are per tail's "next item to return" definition.
                AddChapter(newChapter, out returnTail);
                Debug.Assert(returnTail >= originalTail, $"returnTail {returnTail} must be >= originalTail after AddChapter");
                return returnTail;
            }
        }

        /// <summary>
        /// Add a chapter. <paramref name="newChapterIndex"/> has been incremented to be the next chapter after the last non-null chapter.
        /// </summary>
        private void AddChapter(int newChapterIndex, out int returnTail)
        {
            // We should only be here after we have verified that we need to grow the book or allocate the newChapterIndex, and only one thread should
            // be here due to the "latch" described above. If we are reusing a book, this should already have passed the "chapter is not null" test
            // and we wouldn't be here.
            Debug.Assert(newChapterIndex >= book.Length || book[newChapterIndex] is null, $"Trying to allocate an existing chapter {newChapterIndex}");
            var firstPageInChapter = newChapterIndex << MultiLevelPageArray.ChapterSizeBits;

            try
            {
                // First see if we need to grow the book.
                if (newChapterIndex == book.Length)
                {
                    var newBook = new TElement[book.Length * 2][];
                    Array.Copy(book, newBook, book.Length);
                    book = newBook;
                }

                // Now allocate the new chapter.
                var newChapter = new TElement[MultiLevelPageArray.ChapterSize];

                // Before setting the new chapter into the book, set tail to the second page in the new chapter as "next to return", and we will return
                // the first page after setting the chapter. This ensures that other threads entering Allocate() will see the second page as tail and
                // the chapter as null, so will not try to increment tail until the state is consistent between book[newChapterIndex] and tail.
                tail = firstPageInChapter + 1;
                if (Interlocked.CompareExchange(ref book[newChapterIndex], newChapter, null) != null)
                    throw new TsavoriteException("Unexpected multiple threads in AddChapter");
                returnTail = firstPageInChapter;
            }
            catch
            {
                // Restore tail to the first index in the newChapter. This keeps book[newChapterIndex[0]] as the next page to be returned, and we will
                // retry from the beginning on the next Allocate() iteration (on a different thread, as we're re-throwing (probably OOM) on this one).
                tail = firstPageInChapter;
                throw;
            }
        }

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
            Debug.Assert(index < tail, $"Get(): index {index} must be less than tail {tail}");
            var localBook = book;   // Temp copy as 'book' may be reallocated while we do this (but the chapter indexing remains unchanged and the chapter remains valid).

            var chapterIndex = index >> MultiLevelPageArray.ChapterSizeBits;
            var pageIndex = index & MultiLevelPageArray.PageIndexMask;
            Debug.Assert(localBook[chapterIndex] is not null, $"index {index} out of range of chapters {chapterIndex}");
            return localBook[chapterIndex][pageIndex];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int index, TElement element)
        {
            Debug.Assert(index < tail, $"Set(): index {index} must be less than tail {tail}");
            var localBook = book;   // Temp copy as 'book' may be reallocated while we do this (but the chapter indexing remains unchanged and the chapter remains valid).

            var chapterIndex = index >> MultiLevelPageArray.ChapterSizeBits;
            var pageIndex = index & MultiLevelPageArray.PageIndexMask;
            Debug.Assert(localBook[chapterIndex] is not null, $"index {index} out of range of chapters {chapterIndex}");
            localBook[chapterIndex][pageIndex] = element;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear(int retainedChapterCount = 1 << MultiLevelPageArray.PrimaryClearRetainedChapterSizeBits)
        {
            if (!IsInitialized || tail == 0)
                return;

            // Tail is the next item to return, so may be the first item in a chapter that may still be null--or may be past end of book.
            var lastChapterIndex = (tail - 1) >> MultiLevelPageArray.ChapterSizeBits;
            for (var chapter = 0; chapter <= lastChapterIndex; chapter++)
            {
                Array.Clear(book[chapter], 0, MultiLevelPageArray.ChapterSize);
                if (chapter > retainedChapterCount)
                    book[chapter] = null;
            }

            tail = InitialTail;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear(Action<TElement> action, int retainedChapterCount = 1 << MultiLevelPageArray.PrimaryClearRetainedChapterSizeBits)
        {
            if (!IsInitialized || tail == 0)
                return;

            // Tail is the next item to return, so may be the first item in a chapter that may still be null--or may be past end of book.
            var lastChapterIndex = (tail - 1) >> MultiLevelPageArray.ChapterSizeBits;
            var lastPageIndex = (tail - 1) & MultiLevelPageArray.PageIndexMask;
            for (var chapter = 0; chapter <= lastChapterIndex; chapter++)
            {
                var maxPage = chapter < lastChapterIndex ? MultiLevelPageArray.ChapterSize : lastPageIndex;
                for (var page = 0; page < maxPage; page++)
                {
                    // Note: 'action' must check for null/default.
                    action(book[chapter][page]);
                    book[chapter][page] = default;
                }
                if (chapter > retainedChapterCount)
                    book[chapter] = null;
            }
            tail = InitialTail;
        }

        /// <inheritdoc/>
        public override string ToString() => $"Tail: {tail}, IsInit: {IsInitialized}, book.Len: {(book is not null ? book.Length : "null")}";
    }
}