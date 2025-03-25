// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{

    /// <summary>
    /// Default simple functions base class with TInput and TOutput both being IGarnetObject; it assume there is never a Span instead of an Object
    /// </summary>
    public class SimpleGarnetObjectSessionFunctions : SessionFunctionsBase<IGarnetObject, IGarnetObject, Empty>
    {
        /// <inheritdoc/>
        public override bool ConcurrentReader(ref LogRecord logRecord, ref IGarnetObject input, ref IGarnetObject output, ref ReadInfo readInfo)
        {
            if (logRecord.ValueIsObject)
            {
                output = (IGarnetObject)logRecord.ValueObject;
                return true;
            }
            return false; // TOOD create an IGarnetObject from the serialized bytes
        }

        /// <inheritdoc/>
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref IGarnetObject input, ref IGarnetObject output, ref ReadInfo readInfo)
        {
            if (srcLogRecord.ValueIsObject)
            {
                output = (IGarnetObject)srcLogRecord.ValueObject;
                return true;
            }
            return false; // TOOD create an IGarnetObject from the serialized bytes
        }

        public override bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, IHeapObject srcValue, ref IGarnetObject output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var result = base.SingleWriter(ref dstLogRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);
            if (result)
                output = (IGarnetObject)srcValue;
            return result;
        }

        public override bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, IHeapObject srcValue, ref IGarnetObject output, ref UpsertInfo upsertInfo)
        {
            var result = base.ConcurrentWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = (IGarnetObject)srcValue;
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            var result = dstLogRecord.TrySetValueObject(input, ref sizeInfo);
            if (result)
                output = input;
            return result;
        }
        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo or WriteReason
            var upsertInfo = new UpsertInfo();
            return base.SingleCopyWriter(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref input, ref output, ref upsertInfo, WriteReason.Upsert);
        }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo
            var upsertInfo = new UpsertInfo();
            return ConcurrentWriter(ref logRecord, ref sizeInfo, ref input, input, ref output, ref upsertInfo);
        }
    }
}
