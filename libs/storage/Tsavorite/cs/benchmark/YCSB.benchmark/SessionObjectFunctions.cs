// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public sealed class SessionObjectFunctions : SpanByteFunctions<Empty>
    {
        internal bool objectMode;
        /// <inheritdoc />
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            if (!srcLogRecord.Info.ValueIsObject)
            {
                // Copy only the first cache line for more interpretable results
                srcLogRecord.ValueSpan.Slice(0, 32).CopyTo(output.SpanByte.Span);
            }
            else                                    // Slice the output because it is a larger buffer
                output.SpanByte.AsSpan(0, sizeof(long)).AsRef<long>() = ((ObjectValue)srcLogRecord.ValueObject).value;
            return true;
        }

        /// <inheritdoc/>
        public override bool InPlaceWriter(ref LogRecord logRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            if (!logRecord.Info.ValueIsObject)      // If !ValueIsObject, the destination data length, either inline or out-of-line, should already be sufficient
                input.CopyTo(logRecord.ValueSpan);
            else                                    // Slice the input because it comes from a larger buffer
                ((ObjectValue)logRecord.ValueObject).value = input.ReadOnlySpan.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            if (!dstLogRecord.Info.ValueIsObject)
            {
                if (dstLogRecord.Info.ValueIsInline && input.Length <= dstLogRecord.ValueSpan.Length)
                    input.CopyTo(dstLogRecord.ValueSpan);
                else    // process overflow
                    return dstLogRecord.TrySetValueSpanAndPrepareOptionals(input.ReadOnlySpan, in sizeInfo);
            }
            else
            {
                var objValue = new ObjectValue { value = input.ReadOnlySpan.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value };
                return dstLogRecord.TrySetValueObjectAndPrepareOptionals(objValue, in sizeInfo);
            }
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ref PinnedSpanByte input)
        {
            if (objectMode)
                return new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
            return base.GetUpsertFieldInfo(key, ref input);
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo) => throw new TsavoriteException("InitialUpdater not implemented for YCSB");

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            => InPlaceUpdater(ref dstLogRecord, ref input, ref output, ref rmwInfo);

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            if (!logRecord.Info.ValueIsObject)      // If !ValueIsObject, the destination data length, either inline or out-of-line, should already be sufficient
                input.CopyTo(logRecord.ValueSpan);
            else                                    // Slice the input because it comes from a larger buffer
                ((ObjectValue)logRecord.ValueObject).value = input.ReadOnlySpan.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }
    }
}