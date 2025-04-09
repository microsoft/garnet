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
        public override bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref IGarnetObject input, ref IGarnetObject output, ref ReadInfo readInfo)
        {
            if (srcLogRecord.ValueIsObject)
            {
                output = (IGarnetObject)srcLogRecord.ValueObject;
                return true;
            }
            return false; // TODO create an IGarnetObject from the serialized bytes
        }

        public override bool InitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, IHeapObject srcValue, ref IGarnetObject output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var result = base.InitialWriter(ref dstLogRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);
            if (result)
                output = (IGarnetObject)srcValue;
            return result;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, IHeapObject srcValue, ref IGarnetObject output, ref UpsertInfo upsertInfo)
        {
            var result = base.InPlaceWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
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
            return base.InitialWriter(ref dstLogRecord, ref sizeInfo, ref input, ref srcLogRecord, ref output, ref upsertInfo);
        }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo
            var upsertInfo = new UpsertInfo();
            return InPlaceWriter(ref logRecord, ref sizeInfo, ref input, input, ref output, ref upsertInfo);
        }
    }
}
