// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions for <see cref="SpanByte"/> Value and Input; <see cref="SpanByteAndMemory"/> Output; and specified <typeparamref name="TContext"/>
    /// </summary>
    public class SpanByteFunctions<TContext> : SessionFunctionsBase<SpanByte, SpanByteAndMemory, TContext>
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
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref SpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            srcLogRecord.ValueSpan.CopyTo(ref output, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public override bool ConcurrentReader(ref LogRecord<SpanByte> logRecord, ref SpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            logRecord.ValueSpan.CopyTo(ref output, memoryPool);
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref SpanByte input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = input.Length };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref SpanByte input)
            => new() { KeyDataSize = key.Length, ValueDataSize = input.Length };
        /// <inheritdoc/>
        public override RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref SpanByte input)
            => new() { KeyDataSize = key.Length, ValueDataSize = value.Length };

        /// <inheritdoc />
        public override void ConvertOutputToHeap(ref SpanByte input, ref SpanByteAndMemory output)
        {
            // Currently the default is a no-op; the derived class inspects 'input' to decide whether to ConvertToHeap().
            //output.ConvertToHeap();
        }
    }

    /// <summary>
    /// Callback functions for <see cref="SpanByte"/> input; <see cref="Empty"/> TOutput and TContext .
    /// Used for RMW operations during Compaction.
    /// </summary>
    public class SimpleRMWSpanByteFunctions : SessionFunctionsBase<SpanByte, SpanByte, Empty, Empty>
    {
        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref Empty output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueSpan(input, ref sizeInfo);

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref Empty output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueSpan(srcLogRecord.ValueSpan, ref sizeInfo);

        /// <inheritdoc/>
        // The default implementation of IPU simply writes input to destination, if there is space
        public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref Empty output, ref RMWInfo rmwInfo)
            => logRecord.TrySetValueSpan(input, ref sizeInfo);

        /// <summary>
        /// Length of resulting object when doing RMW with given value and input. Here we set the length
        /// to the max of input and old value lengths. You can provide a custom implementation for other cases.
        /// </summary>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref SpanByte input)
            => new()
            {
                KeyDataSize = srcLogRecord.Key.Length,
                ValueDataSize = srcLogRecord.ValueSpan.Length,
                HasETag = srcLogRecord.Info.HasETag,
                HasExpiration = srcLogRecord.Info.HasExpiration
            };

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref SpanByte input)
            => new()
            {
                KeyDataSize = key.Length,
                ValueDataSize = input.Length
            };

        /// <summary>
        /// Length of resulting object when doing Upsert with given value and input. Here we set the length to the
        /// length of the provided value, ignoring input. You can provide a custom implementation for other cases.
        /// </summary>
        public override RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref SpanByte input)
            => new()
            {
                KeyDataSize = key.Length,
                ValueDataSize = input.Length
            };
    }
}