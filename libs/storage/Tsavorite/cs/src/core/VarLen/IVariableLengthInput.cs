// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for variable length Inputs to RMW.
    /// </summary>
    public interface IVariableLengthInput<TInput>
    {
        /// <summary>Length of resulting value object when performing RMW modification of value using given input</summary>
        RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            ;

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            ;

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref TInput input)
        where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            ;

        /// <summary>Length of value object, when populated by Upsert using given log record</summary>
        RecordFieldInfo GetUpsertFieldInfo<TKey, TSourceLogRecord>(TKey key, in TSourceLogRecord inputLogRecord, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord;
    }
}