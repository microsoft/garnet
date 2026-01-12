// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions for <see cref="ReadOnlySpan{_byte_}"/> Value and <see cref="PinnedSpanByte"/> Input; <see cref="SpanByteAndMemory"/> Output; and specified <typeparamref name="TContext"/>
    /// </summary>
    public class SpanByteFunctions<TContext> : SessionFunctionsBase<PinnedSpanByte, SpanByteAndMemory, TContext>
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
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            srcLogRecord.ValueSpan.CopyTo(ref output, memoryPool);
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = input.Length };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref PinnedSpanByte input)
            => new() { KeySize = key.Length, ValueSize = input.Length };
        /// <inheritdoc/>
        public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref PinnedSpanByte input)
            => new() { KeySize = key.Length, ValueSize = value.Length };
        /// <inheritdoc/>
        public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref PinnedSpanByte input)
            => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };

        /// <inheritdoc />
        public override void ConvertOutputToHeap(ref PinnedSpanByte input, ref SpanByteAndMemory output)
        {
            // Currently the default is a no-op; the derived class inspects 'input' to decide whether to ConvertToHeap().
            //output.ConvertToHeap();
        }
    }
}