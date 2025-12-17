// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Default simple functions base class with TInput and TOutput both being IGarnetObject; it assumes the Value is always an Object, never a Span.
    /// </summary>
    public class SimpleGarnetObjectSessionFunctions : SessionFunctionsBase<IGarnetObject, IGarnetObject, Empty>
    {
        /// <inheritdoc/>
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref IGarnetObject input, ref IGarnetObject output, ref ReadInfo readInfo)
        {
            if (srcLogRecord.Info.ValueIsObject)
            {
                output = (IGarnetObject)srcLogRecord.ValueObject;
                return true;
            }
            return false; // TODO: possibly create an IGarnetObject from the serialized bytes
        }

        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref IGarnetObject input, IHeapObject srcValue, ref IGarnetObject output, ref UpsertInfo upsertInfo)
        {
            var result = base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = (IGarnetObject)srcValue;
            return result;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref IGarnetObject input, IHeapObject srcValue, ref IGarnetObject output, ref UpsertInfo upsertInfo)
        {
            var result = base.InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = (IGarnetObject)srcValue;
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            var result = dstLogRecord.TrySetValueObjectAndPrepareOptionals(input, in sizeInfo);
            if (result)
                output = input;
            return result;
        }
        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo
            var upsertInfo = new UpsertInfo();
            return base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, in srcLogRecord, ref output, ref upsertInfo);
        }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo
            var upsertInfo = new UpsertInfo();
            return InPlaceWriter(ref logRecord, in sizeInfo, ref input, input, ref output, ref upsertInfo);
        }
    }
}