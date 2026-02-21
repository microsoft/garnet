// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface to implement the Disposer component of <see cref="IStoreFunctions{Key, Value}"/>
    /// </summary>
    public interface IRecordDisposer<TKey, TValue>
    {
        /// <summary>
        /// If true, <see cref="DisposeRecord(ref TKey, ref TValue, DisposeReason, int)"/> with <see cref="DisposeReason.PageEviction"/> 
        /// is called on page evictions from both readcache and main log. Otherwise, the user can register an Observer and do any needed disposal there.
        /// </summary>
        public bool DisposeOnPageEviction { get; }

        /// <summary>
        /// Dispose the Key and Value of a record, if necessary. See comments in <see cref="IStoreFunctions{Key, Value}.DisposeRecord(ref Key, ref Value, DisposeReason, int)"/> for details.
        /// </summary>
        void DisposeRecord(ref TKey key, ref TValue value, DisposeReason reason, int newKeySize);
    }

    /// <summary>
    /// Default no-op implementation if <see cref="IRecordDisposer{Key, Value}"/>
    /// </summary>
    /// <remarks>It is appropriate to call methods on this instance as a no-op.</remarks>
    public struct DefaultRecordDisposer<TKey, TValue> : IRecordDisposer<TKey, TValue>
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly DefaultRecordDisposer<TKey, TValue> Instance = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly bool DisposeOnPageEviction => false;

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly void DisposeRecord(ref TKey key, ref TValue value, DisposeReason reason, int newKeySize)
        {
            Debug.Assert(typeof(TKey) != typeof(SpanByte) && typeof(TValue) != typeof(SpanByte), "Must use SpanByteRecordDisposer");
        }
    }

    /// <summary>
    /// Default no-op implementation if <see cref="IRecordDisposer{Key, Value}"/> for SpanByte
    /// </summary>
    public struct SpanByteRecordDisposer : IRecordDisposer<SpanByte, SpanByte>
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly SpanByteRecordDisposer Instance = new();

        /// <summary>
        /// Assumes the key and value have no need of Dispose(), and does nothing.
        /// </summary>
        public readonly bool DisposeOnPageEviction => false;

        /// <summary>
        /// If <paramref name="reason"/> is <see cref="DisposeReason.RevivificationFreeList"/> and <paramref name="newKeySize"/> is >= 0,
        /// this adjusts the key (and if necessary value) space as needed to preserve log zero-init correctness.
        /// Otherwise the key and value have no need of disposal, and this does nothing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void DisposeRecord(ref SpanByte key, ref SpanByte value, DisposeReason reason, int newKeySize)
        {
            // We don't have to do anything with the Value unless the new key size requires adjusting the key length.
            // newKeySize == -1 means we are preserving the existing key (e.g. for in-chain revivification).
            if (reason != DisposeReason.RevivificationFreeList || newKeySize < 0)
                return;

            var oldKeySize = Utility.RoundUp(key.TotalSize, Constants.kRecordAlignment);

            // We are changing the key size (e.g. revivification from the freelist with a new key).
            // Our math here uses record alignment of keys as in the allocator, and assumes this will always be at least int alignment.
            newKeySize = Utility.RoundUp(newKeySize, Constants.kRecordAlignment);
            int keySizeChange = newKeySize - oldKeySize;
            if (keySizeChange == 0)
                return;

            // We are growing or shrinking. We don't care (here or in SingleWriter, InitialUpdater, CopyUpdater) what is inside the Key and Value,
            // as long as we don't leave nonzero bytes after the used value space. So we just need to make sure the Value space starts immediately
            // after the new key size. SingleWriter et al. will do the ShrinkSerializedLength on Value as needed.
            if (keySizeChange < 0)
            {
                // We are shrinking the key; the Value of the new record will start after key + newKeySize, so set the new value length there.
                *(int*)((byte*)Unsafe.AsPointer(ref key) + newKeySize) = value.Length - keySizeChange; // minus negative => plus positive
            }
            else
            {
                // We are growing the key; the Value of the new record will start somewhere in the middle of where the old Value was, so set the new value length there.
                *(int*)((byte*)Unsafe.AsPointer(ref value) + keySizeChange) = value.Length - keySizeChange;
            }

            // NewKeySize is (newKey).TotalSize.
            key.Length = newKeySize - sizeof(int);
        }
    }
}