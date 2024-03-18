// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions for SpanByte Key, Value, Input; SpanByteAndMemory Output; and specified Context
    /// </summary>
    public class SpanByteFunctions<Context> : SpanByteFunctions<SpanByteAndMemory, Context>
    {
        private protected readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteFunctions(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            value.CopyTo(ref dst, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            value.CopyTo(ref dst, memoryPool);
            return true;
        }
    }

    /// <summary>
    /// Callback functions for SpanByte key, value, input; specified Output and Context
    /// </summary>
    public class SpanByteFunctions<Output, Context> : FunctionsBase<SpanByte, SpanByte, SpanByte, Output, Context>
    {
        /// <inheritdoc />
        public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

        /// <summary>
        /// Utility function for SpanByte copying, Upsert version.
        /// </summary>
        public static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            // First get the full record length and clear it from the extra value space (if there is any). 
            // This ensures all bytes after the used value space are 0, which retains log-scan correctness.

            // For non-in-place operations, the new record may have been revivified, so standard copying procedure must be done;
            // For SpanByte we don't implement DisposeForRevivification, so any previous value is still there, and thus we must
            // zero unused value space to ensure log-scan correctness, just like in in-place updates.

            // IMPORTANT: usedValueLength and fullValueLength use .TotalSize, not .Length, to account for the leading "Length" int.
            upsertInfo.ClearExtraValueLength(ref recordInfo, ref dst, dst.TotalSize);

            // We want to set the used and extra lengths and Filler whether we succeed (to the new length) or fail (to the original length).
            var result = src.TrySafeCopyTo(ref dst, upsertInfo.FullValueLength);
            upsertInfo.SetUsedValueLength(ref recordInfo, ref dst, dst.TotalSize);
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref input, ref value, ref rmwInfo, ref recordInfo);

        /// <inheritdoc/>
        public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref oldValue, ref newValue, ref rmwInfo, ref recordInfo);

        /// <inheritdoc/>
        // The default implementation of IPU simply writes input to destination, if there is space
        public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref input, ref value, ref rmwInfo, ref recordInfo);

        /// <summary>
        /// Length of resulting object when doing RMW with given value and input. Here we set the length
        /// to the max of input and old value lengths. You can provide a custom implementation for other cases.
        /// </summary>
        public override int GetRMWModifiedValueLength(ref SpanByte t, ref SpanByte input)
            => sizeof(int) + (t.Length > input.Length ? t.Length : input.Length);

        /// <inheritdoc/>
        public override int GetRMWInitialValueLength(ref SpanByte input) => input.TotalSize;

        /// <summary>
        /// Utility function for SpanByte copying, RMW version.
        /// </summary>
        public static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            // See comments in upsertInfo overload of this function.
            rmwInfo.ClearExtraValueLength(ref recordInfo, ref dst, dst.TotalSize);
            var result = src.TrySafeCopyTo(ref dst, rmwInfo.FullValueLength);
            rmwInfo.SetUsedValueLength(ref recordInfo, ref dst, dst.TotalSize);
            return result;
        }

        /// <inheritdoc/>
        /// <remarks>Avoids the "value = default" for added tombstone record, which do not have space for the payload</remarks>
        public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        /// <inheritdoc />
        public override unsafe void DisposeForRevivification(ref SpanByte key, ref SpanByte value, int newKeySize)
        {
            var oldKeySize = RoundUp(key.TotalSize, SpanByteAllocator.kRecordAlignment);

            // We don't have to do anything with the Value unless the new key size requires adjusting the key length.
            // newKeySize == -1 means we are preserving the existing key (e.g. for in-chain revivification).
            if (newKeySize < 0)
                return;

            // We are changing the key size (e.g. revivification from the freelist with a new key).
            // Our math here uses record alignment of keys as in the allocator, and assumes this will always be at least int alignment.
            newKeySize = RoundUp(newKeySize, SpanByteAllocator.kRecordAlignment);
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