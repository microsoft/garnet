// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for variable length Inputs to RMW; only implemented for <typeparamref name="TValue"/> of <see cref="SpanByte"/>.
    /// </summary>
    public interface IVariableLengthInput<TValue, TInput>
    {
        /// <summary>Length of resulting value object when performing RMW modification of value using given input</summary>
        RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : IReadOnlyLogRecord;

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        RecordFieldInfo GetRMWInitialFieldInfo(ref TInput input);

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        RecordFieldInfo GetUpsertFieldInfo(TValue value, ref TInput input);
    }
}