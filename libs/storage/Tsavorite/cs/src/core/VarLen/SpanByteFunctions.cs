// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions for <see cref="SpanByte"/> Key, Value, Input; <see cref="SpanByteAndMemory"/> Output; and specified <typeparamref name="TContext"/>
    /// </summary>
    public class SpanByteFunctions<TContext> : SpanByteFunctions<SpanByteAndMemory, TContext>
    {
        protected readonly MemoryPool<byte> memoryPool;

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

        /// <inheritdoc />
        public override void ConvertOutputToHeap(ref SpanByte input, ref SpanByteAndMemory output)
        {
            // Currently the default is a no-op; the derived class inspects 'input' to decide whether to ConvertToHeap().
            //output.ConvertToHeap();
        }
    }

    /// <summary>
    /// Callback functions for <see cref="SpanByte"/> key, value; specified <typeparamref name="TInput"/>, <typeparamref name="TOutput"/>, and <typeparamref name="TContext"/>
    /// </summary>
    public class SpanByteFunctions<TInput, TOutput, TContext> : SessionFunctionsBase<SpanByte, SpanByte, TInput, TOutput, TContext>
    {
        /// <inheritdoc />
        public override bool SingleWriter(ref SpanByte key, ref TInput input, ref SpanByte src, ref SpanByte dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref SpanByte key, ref TInput input, ref SpanByte src, ref SpanByte dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

        /// <summary>
        /// Utility function for <see cref="SpanByte"/> copying, Upsert version.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo, long metadata = 0)
        {
            // First get the full record length and clear it from the extra value space (if there is any). 
            // This ensures all bytes after the used value space are 0, which retains log-scan correctness.

            // For non-in-place operations, the new record may have been revivified, so standard copying procedure must be done;
            // For SpanByte we don't implement DisposeForRevivification, so any previous value is still there, and thus we must
            // zero unused value space to ensure log-scan correctness, just like in in-place updates.

            // IMPORTANT: usedValueLength and fullValueLength use .TotalSize, not .Length, to account for the leading "Length" int.
            upsertInfo.ClearExtraValueLength(ref recordInfo, ref dst, dst.TotalSize);

            // We want to set the used and extra lengths and Filler whether we succeed (to the new length) or fail (to the original length).
            var result = src.TrySafeCopyTo(ref dst, upsertInfo.FullValueLength, metadata);
            upsertInfo.SetUsedValueLength(ref recordInfo, ref dst, dst.TotalSize);
            return result;
        }

        /// <summary>
        /// Utility function for <see cref="SpanByte"/> copying, RMW version.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    }

    /// <summary>
    /// Callback functions for <see cref="SpanByte"/> key, value, input; specified <typeparamref name="TOutput"/> and <typeparamref name="TContext"/>
    /// </summary>
    public class SpanByteFunctions<TOutput, TContext> : SpanByteFunctions<SpanByte, TOutput, TContext>
    {
        /// <inheritdoc/>
        public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref input, ref value, ref rmwInfo, ref recordInfo);

        /// <inheritdoc/>
        public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref oldValue, ref newValue, ref rmwInfo, ref recordInfo);

        /// <inheritdoc/>
        // The default implementation of IPU simply writes input to destination, if there is space
        public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
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
        /// Length of resulting object when doing Upsert with given value and input. Here we set the length to the
        /// length of the provided value, ignoring input. You can provide a custom implementation for other cases.
        /// </summary>
        public override int GetUpsertValueLength(ref SpanByte t, ref SpanByte input)
            => t.TotalSize;
    }
}