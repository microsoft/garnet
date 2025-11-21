// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for variable length Inputs to Upsert and RMW.
    /// </summary>
    public interface IVariableLengthInput<TInput>
    {
        /// <summary>Length of resulting value object when performing RMW modification of value using given input</summary>
        RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TInput input);

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input);

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input);

        /// <summary>Length of value object, when populated by Upsert using given log record</summary>
        RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord;
    }
}