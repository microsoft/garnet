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

        private const int InitialTail = -1;     // Start at -1 so Allocate() sets it to 0
        internal int tail = InitialTail;

        public bool IsInitialized => book is not null;

        public int Count => tail + 1;   // +1 because we start at -1 and increment before returning.

        public int Allocate()
        {
            while (tail == InitialTail)
            {
                // The book may be non-null due to Clear(), e.g. when we wrap around in the log to page 0.
                // Return the 0 here; other threads seeing this set to non-InitialTail and breaking will return an incremented tail.
                if (book is not null && Interlocked.CompareExchange(ref tail, 0, InitialTail) == InitialTail)
                    return 0;

                // Two-step process in case the element allocation throws; book will still be null.
                var newBook = new TElement[MultiLevelPageArray.InitialBookSize][];
                newBook[0] = new TElement[MultiLevelPageArray.ChapterSize];

                // Multiple threads can hit this at the same time, so use Interlocked to ensure that only one thread sets book;
                // others will just drop through to normal handling as book will have been set by that one thread.
                if (Interlocked.CompareExchange(ref book, newBook, null) == null)
                    return tail = 0;
                _ = Thread.Yield();
            }

            while (true)
            {
                var originalTail = tail;
                var originalChapter = originalTail >> MultiLevelPageArray.ChapterSizeBits;
                if (originalChapter >= book.Length || book[originalChapter] is null)
                {
                    // One or more other threads has incremented tail into a new, not-yet-allocated chapter, which means one owns the new-chapter "latch".
                    // Don't increment tail; just wait for that owning thread to allocate the new chapter.
                    _ = Thread.Yield();
                    continue;
                }

                // If we are here, we did not need to allocate a new chapter, or we own the new-chapter "latch". We'll return the incremented value of tail.
                var newTail = Interlocked.Increment(ref tail);
                var newChapter = newTail >> MultiLevelPageArray.ChapterSizeBits;

                Debug.Assert(newChapter >= originalChapter, $"newChapter {newChapter} should not be < originalChapter {originalChapter}");
                if (newChapter > originalChapter)
                {
                    // We are on a new chapter, and possibly need to grow the book. If we incremented such that the value of newTail is the first page of the next chapter,
                    // then we are the first to cross the threshold to the new chapter and we "own the latch" for allocating that new chapter. If the increment put it past that,
                    // then someone else has the threshold increment and is allocating the new chapter, and we must wait until that chapter is allocated.
                    var newPage = newTail & MultiLevelPageArray.PageIndexMask;
                    if (newPage == 0)
                    {
                        AddChapter(newChapter);
                    }
                    else
                    {
                        // Wait for the thread that owns the new-page "latch" to allocate the new chapter. TODO: Tail is reset on OOM; need to break out of the inner loop and retry if that happens.
                        while (newChapter >= book.Length || book[newChapter] is null)
                            _ = Thread.Yield(); // TODO consider SpinWait.SpinOnce() with backoff
                    }
                }

                Debug.Assert(newTail <= tail, $"newTail {newTail} should not be > tail {tail}");
                Debug.Assert(book[newChapter] is not null, $"Expected new chapter {newChapter} to be non-null pt 2");
                return newTail;
            }
        }

        /// <summary>
        /// Add a chapter. <paramref name="newChapterIndex"/> has been incremented to be the next chapter after the last non-null chapter.
        /// </summary>
        private void AddChapter(int newChapterIndex)
        {
            // If the chapter is already allocated (e.g. reusing on a new Allocator page), we don't need to do anything, and especially
            // must not change tail, because the caller will see a non-null chapter and will increment tail and expect that increment to persist.
            if (newChapterIndex < book.Length && book[newChapterIndex] is not null)
                return;

            try
            {
                if (newChapterIndex == book.Length)
                {
                    // We need to grow the book.
                    var newBook = new TElement[book.Length * 2][];
                    Array.Copy(book, newBook, book.Length);
                    book = newBook;
                }

                book[newChapterIndex] = new TElement[MultiLevelPageArray.ChapterSize];
            }
            catch
            {
                // Restore tail to the last index on the last non-null chapter--that is, the chapter before chapterIndex (this cannot set it to -1 because
                // we allocate the book and first chapter in a different block). This lets the "increment tail into next chapter" loop execute and if there
                // are threads spinning in that, or others enter after this throw, one will be released to try again (possibly also encountering OOM and throwing).
                tail = (newChapterIndex << MultiLevelPageArray.ChapterSizeBits) - 1;
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
            Debug.Assert(index <= tail, $"index {index} out of range of tail {tail}");
            var localBook = book;   // Temp copy as 'book' may be reallocated while we do this (but the chapter indexing remains unchanged and the chapter remains valid).

            var chapterIndex = index >> MultiLevelPageArray.ChapterSizeBits;
            var pageIndex = index & MultiLevelPageArray.PageIndexMask;
            Debug.Assert(localBook[chapterIndex] is not null, $"index {index} out of range of chapters {chapterIndex}");
            return localBook[chapterIndex][pageIndex];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int index, TElement element)
        {
            Debug.Assert(index <= tail, $"index {index} out of range of tail {tail}");
            var localBook = book;   // Temp copy as 'book' may be reallocated while we do this (but the chapter indexing remains unchanged and the chapter remains valid).

            var chapterIndex = index >> MultiLevelPageArray.ChapterSizeBits;
            var pageIndex = index & MultiLevelPageArray.PageIndexMask;
            Debug.Assert(localBook[chapterIndex] is not null, $"index {index} out of range of chapters {chapterIndex}");
            localBook[chapterIndex][pageIndex] = element;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            if (!IsInitialized)
                return;
            var lastChapterIndex = tail >> MultiLevelPageArray.ChapterSizeBits;
            for (int chapter = 0; chapter <= lastChapterIndex; chapter++)
                Array.Clear(book[chapter], 0, MultiLevelPageArray.ChapterSize);
            tail = InitialTail;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear(Action<TElement> action)
        {
            if (!IsInitialized)
                return;
            var lastChapterIndex = tail >> MultiLevelPageArray.ChapterSizeBits;
            var lastPageIndex = tail & MultiLevelPageArray.PageIndexMask;
            for (int chapter = 0; chapter <= lastChapterIndex; chapter++)
            {
                var maxPage = chapter < lastChapterIndex ? MultiLevelPageArray.ChapterSize : lastPageIndex;
                for (int page = 0; page < maxPage; page++)
                {
                    // Note: 'action' must check for null/default.
                    action(book[chapter][page]);
                    book[chapter][page] = default;
                }
            }
            tail = InitialTail;
        }

        /// <inheritdoc/>
        public override string ToString() => $"Tail: {tail}, IsInitialized: {IsInitialized}";
    }
}