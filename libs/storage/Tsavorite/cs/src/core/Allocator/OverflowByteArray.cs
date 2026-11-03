// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>A byte[] wrapper that encodes start and end offsets of the actual data in the first sizeof(<see cref="OverflowHeader"/>) bytes in the array.</summary>
    /// <remarks>Used primarily for sector-aligned reads directly into the overflow byte[].</remarks>
    public struct OverflowByteArray
    {
        /// <summary>Define the header of an overflow allocation. Overflow allocations are typically large so use a full int to allow greater than 64k offsets,
        /// which makes it possible to read more information with a single IO and then copy it out to other destinations. Sector sizes may be up to 64k on NTFS systems,
        /// which is sizeof(ushort) bytes, so the use of full ints removes boundary concerns (e.g. reading a value followed by optional bytes may cross a sector boundary,
        /// in which case we need an end offset greater than a single sector).</summary>
        /// <remarks>
        /// <para>The publication fields serve a value array the way <see cref="HeapObjectBase"/>'s phase word serves an object: a flush or a Snapshot
        /// checkpoint's capture holds the claim from its object-log write through its stamps, and the phase says which flush last published the
        /// record. Unlike an object, an array can be shared: a CopyUpdate that keeps the value's length gives the new record the source's array. So
        /// the header also records the owner, the address of the record that publication describes; see <see cref="ClaimForPublication"/>.</para>
        /// <para>The size keeps the data 8-byte aligned.</para>
        /// </remarks>
        [StructLayout(LayoutKind.Sequential)]
        struct OverflowHeader
        {
            internal const int Size = 4 * sizeof(int) + sizeof(long);
            internal int startOffset, endOffset;

            /// <summary>The publication phase, encoded as in <see cref="HeapObjectBase"/>: a <see cref="SerializationPhase"/> in the low bits and,
            /// for <see cref="SerializationPhase.SNAPSHOT_STAMPED"/>, the Snapshot checkpoint's tag above them; or <see cref="SentinelWord"/>.</summary>
            internal int phaseWord;
            internal int reserved;

            /// <summary>The logical address of the record that <see cref="phaseWord"/> describes, when it is stamped.</summary>
            internal long owner;
        }

        /// <summary>The phase word of a Snapshot checkpoint's sentinel: an array with no data that never takes a claim.</summary>
        const int SentinelWord = -1;

        internal readonly byte[] Array { get; init; }

        internal readonly bool IsEmpty => Array is null;

        internal readonly int StartOffset => Unsafe.As<byte, OverflowHeader>(ref Array[0]).startOffset + OverflowHeader.Size;

        /// <summary>The total size of the array allocated (includes space for offset values)</summary>
        public readonly int TotalSize => Array.Length;

        /// <summary>The total heap size of the array (includes space for offset values and .net array overhead)</summary>
        public readonly int HeapMemorySize => Array is null ? 0 : Array.Length + MemoryUtils.ByteArrayOverhead;

        readonly int EndOffset => Unsafe.As<byte, OverflowHeader>(ref Array[0]).endOffset;

        internal readonly int Length => Array.Length - StartOffset - EndOffset;

        /// <inheritdoc/>
        public override string ToString() => $"Len {Length}, IsEmpty {IsEmpty}, sOffset {StartOffset}, eOffset {EndOffset}, HeapMemSize {HeapMemorySize}, TotSize {TotalSize}";

        /// <summary>ReadOnlySpan of data between offsets</summary>
        internal readonly ReadOnlySpan<byte> ReadOnlySpan => Array.AsSpan(StartOffset, Length);
        /// <summary>ReadOnlySpan of data between offsets</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsReadOnlySpan(int start)
        {
            var length = Length;
            return start <= length ? Array.AsSpan(StartOffset + start, length - start) : throw new ArgumentOutOfRangeException(nameof(start));
        }
        /// <summary>ReadOnlySpan of data between offsets</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsReadOnlySpan(int start, int len)
        {
            var length = Length;
            return ((ulong)(uint)start + (uint)len <= (uint)length) ? Array.AsSpan(StartOffset + start, len) : throw new ArgumentOutOfRangeException($"start {nameof(start)} + len {len} exceeds length {length}");
        }

        /// <summary>Span of data between offsets</summary>
        internal readonly Span<byte> Span => Array.AsSpan(StartOffset, Length);
        /// <summary>Span of data between offsets</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Span<byte> AsSpan(int start)
        {
            var length = Length;
            return start <= length ? Array.AsSpan(StartOffset + start, length - start) : throw new ArgumentOutOfRangeException(nameof(start));
        }
        /// <summary>ReadOnlySpan of data between offsets</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Span<byte> AsSpan(int start, int len)
        {
            var length = Length;
            return ((ulong)(uint)start + (uint)len <= (uint)length) ? Array.AsSpan(StartOffset + start, len) : throw new ArgumentOutOfRangeException($"start {nameof(start)} + len {len} exceeds length {length}");
        }

        /// <summary>Span of all data, including before and after offsets; this is for aligned Read from the device.</summary>
        internal readonly Span<byte> AlignedReadSpan => Array.AsSpan(OverflowHeader.Size);

        /// <summary>
        /// Get a <see cref="System.Memory{T}"/> view over the value bytes (between <see cref="StartOffset"/> and the end-offset).
        /// </summary>
        /// <remarks>
        /// The returned <see cref="System.Memory{T}"/> aliases the underlying byte[]; consumers that need a stable native pointer
        /// (e.g. for SIMD operations) should call <see cref="System.Memory{T}.Pin"/> for the duration of the operation, otherwise
        /// GC compaction may relocate the array.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Memory<byte> AsMemory() => Array.AsMemory(StartOffset, Length);

        /// <summary>Construct an <see cref="OverflowByteArray"/> from a byte[] allocated by <see cref="OverflowByteArray(int, int, int, bool)"/>.</summary>
        internal OverflowByteArray(byte[] data) => Array = data;

        /// <summary>
        /// Allocate an overflow buffer with a data region of exactly <paramref name="length"/> bytes (start/end offsets zero), for a
        /// caller that will populate the region directly via <see cref="AsSpan(int, int)"/> — e.g. the chunked migration/replication
        /// receiver copying network chunks straight into the record's final overflow buffer (a single up-front allocation).
        /// </summary>
        /// <param name="length">The size in bytes of the data region.</param>
        public static OverflowByteArray AllocateData(int length) => new(length, startOffset: 0, endOffset: 0, zeroInit: false);

        internal OverflowByteArray(int length, int startOffset, int endOffset, bool zeroInit)
        {
            // Allocate with enough extra space for the metadata (offset from start and end)
            Array = !zeroInit
                ? GC.AllocateUninitializedArray<byte>(length + OverflowHeader.Size)
                : (new byte[length + OverflowHeader.Size]);
            ref var header = ref Unsafe.As<byte, OverflowHeader>(ref Array[0]);
            header.startOffset = startOffset;
            header.endOffset = endOffset;
            header.phaseWord = (int)SerializationPhase.REST;
            header.reserved = 0;
            header.owner = 0;
        }

        /// <summary>Create a Snapshot checkpoint's sentinel. A capture points the source record's value slot at it, after giving the value
        /// itself to the CopyUpdate's new record; a flush identifies the capture by it.</summary>
        internal static OverflowByteArray CreateSentinel()
        {
            var sentinel = new OverflowByteArray(0, startOffset: 0, endOffset: 0, zeroInit: true);
            Unsafe.As<byte, OverflowHeader>(ref sentinel.Array[0]).phaseWord = SentinelWord;
            return sentinel;
        }

        /// <summary>The sentinel a CopyUpdate leaves in the value slot of a source whose value no checkpoint captured, when it releases the
        /// value without changing the source's bytes. It reads as an empty value, and a flush writes it as one.</summary>
        internal static readonly OverflowByteArray Released = CreateSentinel();

        /// <summary>Whether this is a sentinel; see <see cref="CreateSentinel"/> and <see cref="Released"/>.</summary>
        internal readonly bool IsSentinel => Volatile.Read(ref Unsafe.As<byte, OverflowHeader>(ref Array[0]).phaseWord) == SentinelWord;

        /// <summary>Wait until no thread holds this array's publication claim, then take it.</summary>
        /// <param name="priorWord">The phase word found; pass it, or the word for what the claim published, to <see cref="ReleasePublicationClaim"/>.</param>
        /// <param name="priorOwner">The address of the record the phase word describes, when it is stamped.</param>
        /// <remarks>
        /// <para>The claim serializes publishers of the records that reference this array: a flush holds it from its object-log write through its
        /// stamps, and a capture holds it until it has detached its source. A stamped phase describes only the record at
        /// <paramref name="priorOwner"/>; for any other record the array is unpublished.</para>
        /// <para>Waits by yielding: a claim is held across an object-log write, which can take as long as copying the array.</para>
        /// </remarks>
        internal readonly void ClaimForPublication(out int priorWord, out long priorOwner)
        {
            ref var header = ref Unsafe.As<byte, OverflowHeader>(ref Array[0]);
            Debug.Assert(Volatile.Read(ref header.phaseWord) != SentinelWord, "A sentinel takes no claim");
            while (true)
            {
                var word = Volatile.Read(ref header.phaseWord);
                if ((word & PhaseMask) == (int)SerializationPhase.SERIALIZING)
                {
                    _ = Thread.Yield();
                    continue;
                }

                // Test only, compiled out of Release builds: a test's hook can run another publisher's claim and release here, between the
                // read of the word and the CAS.
                ObjectFlushInjection.At(ObjectFlushPhase.PublicationClaimSampled, 0);
                if (Interlocked.CompareExchange(ref header.phaseWord, (int)SerializationPhase.SERIALIZING, word) == word)
                {
                    // Read the owner only now that the claim is held. Only a claim holder writes it, so it matches the word this CAS took.
                    // Read before the CAS, it could belong to a publisher that claimed and released this same word in between.
                    priorWord = word;
                    priorOwner = Volatile.Read(ref header.owner);
                    return;
                }
            }
        }

        /// <summary>End a claim taken by <see cref="ClaimForPublication"/>, leaving the phase word <paramref name="word"/> describing the record at
        /// <paramref name="owner"/>.</summary>
        internal readonly void ReleasePublicationClaim(int word, long owner)
        {
            ref var header = ref Unsafe.As<byte, OverflowHeader>(ref Array[0]);
            Debug.Assert((Volatile.Read(ref header.phaseWord) & PhaseMask) == (int)SerializationPhase.SERIALIZING, "ReleasePublicationClaim without a claim");
            Volatile.Write(ref header.owner, owner);
            Volatile.Write(ref header.phaseWord, word);
        }

        const int PhaseMask = (1 << HeapObjectBase.PhaseBits) - 1;

        /// <summary>Increase the offset from the start, e.g. after having extracted the key that was read in the same IO operation as the value.</summary>
        /// <remarks>This is 'readonly' because it does not alter the <see cref="Array"/> array field, only its contents.</remarks>
        internal readonly void AdjustOffsetFromStart(int increment) => Unsafe.As<byte, OverflowHeader>(ref Array[0]).startOffset += increment;
        /// <summary>Increase the offset from the end, e.g. after having extracted the optionals that were read in the same IO operation as the value.</summary>
        /// <remarks>This is 'readonly' because it does not alter the <see cref="Array"/>> array field, only its contents.</remarks>
        internal readonly void AdjustOffsetFromEnd(int increment) => Unsafe.As<byte, OverflowHeader>(ref Array[0]).endOffset += increment;

        internal readonly void SetOffsets(int offsetFromStart, int offsetFromEnd)
        {
            Debug.Assert(offsetFromStart > 0 && offsetFromStart < Array.Length - 1, "offsetFromStart is out of range");
            Debug.Assert(offsetFromEnd > 0 && offsetFromEnd < Array.Length - 1, "offsetFromEnd is out of range");
            Debug.Assert(offsetFromStart < offsetFromEnd, "offsetFromStart must be less than offsetFromEnd");
            ref var header = ref Unsafe.As<byte, OverflowHeader>(ref Array[0]);
            header.startOffset = offsetFromStart;
            header.endOffset = offsetFromEnd;
        }

        /// <summary>Set the leading and trailing allocation slack excluded from the logical payload span. A direct object-log read uses
        /// these offsets to place the payload at the same sector residue as its file position while allowing the aligned device request
        /// to start before and end after the logical payload without a staging allocation.</summary>
        internal readonly void SetAlignedReadOffsets(int offsetFromStart, int offsetFromEnd)
        {
            Debug.Assert(offsetFromStart >= 0 && offsetFromEnd >= 0, "Aligned-read offsets must be non-negative");
            Debug.Assert(offsetFromStart + offsetFromEnd <= Array.Length - OverflowHeader.Size, "Aligned-read offsets exceed allocation");
            ref var header = ref Unsafe.As<byte, OverflowHeader>(ref Array[0]);
            header.startOffset = offsetFromStart;
            header.endOffset = offsetFromEnd;
        }

        /// <summary>Pin the underlying heap object.  It is the caller's responsibility to release the returned <see cref="GCHandle"/>.</summary>
        public readonly GCHandle Pin()
        => GCHandle.Alloc(Array, GCHandleType.Pinned);

        /// <summary>Get the <see cref="ReadOnlySpan{_byte_}"/> of a byte[] allocated by <see cref="OverflowByteArray(int, int, int, bool)"/> constructor.</summary>
        internal static ReadOnlySpan<byte> AsReadOnlySpan(object value) => new OverflowByteArray(Unsafe.As<byte[]>(value)).ReadOnlySpan;

        /// <summary>Get the <see cref="Span{_byte_}"/> of a byte[] allocated by <see cref="OverflowByteArray(int, int, int, bool)"/> constructor.</summary>
        internal static Span<byte> AsSpan(object value) => new OverflowByteArray(Unsafe.As<byte[]>(value)).Span;
    }
}