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

        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueObjectAndPrepareOptionals(input, in sizeInfo))
                return false;
            output = input;
            return true;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, ref IGarnetObject input, ref IGarnetObject output, ref UpsertInfo upsertInfo)
        {
            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new RecordFieldInfo()
                {
                    KeySize = logRecord.Key.Length,
                    ValueSize = ObjectIdMap.ObjectIdSize,
                    ValueIsObject = true
                }
            };
            if (!logRecord.TrySetValueObjectAndPrepareOptionals(input, in sizeInfo))
                return false;
            output = input;
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ref IGarnetObject input)
            => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };

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
            return dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo);
        }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo
            var upsertInfo = new UpsertInfo();
            return InPlaceWriter(ref logRecord, ref input, ref output, ref upsertInfo);
        }
    }
}