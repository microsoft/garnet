// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Eliminates switching on type of value or another variable to determine which overloaded value-taking method to call.
        /// </summary>
        internal interface IUpsertValueSelector
        {
            static abstract RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(TAllocator allocator, ReadOnlySpan<byte> key,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
                where TSourceLogRecord : ISourceLogRecord
                where TVariableLengthInput : IVariableLengthInput<TInput>;

            static abstract bool InitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>;

            static abstract void PostInitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>;

            static abstract bool InPlaceWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>;

            static abstract void PostUpsertOperation<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper, TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions, TEpochAccessor epochAccessor)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                where TEpochAccessor : IEpochAccessor;
        }

        internal struct SpanUpsertValueSelector : IUpsertValueSelector
        {
            public static RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(TAllocator allocator, ReadOnlySpan<byte> key,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
                where TSourceLogRecord : ISourceLogRecord
                where TVariableLengthInput : IVariableLengthInput<TInput>
                => allocator.GetUpsertRecordSize(key, valueSpan, ref input, varlenInput);

            public static bool InitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.InitialWriter(ref logRecord, in sizeInfo, ref input, valueSpan, ref output, ref upsertInfo);

            public static void PostInitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.PostInitialWriter(ref logRecord, in sizeInfo, ref input, valueSpan, ref output, ref upsertInfo);

            public static bool InPlaceWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.InPlaceWriter(ref logRecord, in sizeInfo, ref input, valueSpan, ref output, ref upsertInfo);

            public static void PostUpsertOperation<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper, TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions, TEpochAccessor epochAccessor)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                where TEpochAccessor : IEpochAccessor
                => sessionFunctions.PostUpsertOperation(key, ref input, valueSpan, ref upsertInfo, epochAccessor);
        }

        internal struct ObjectUpsertValueSelector : IUpsertValueSelector
        {
            public static RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(TAllocator allocator, ReadOnlySpan<byte> key,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
                where TSourceLogRecord : ISourceLogRecord
                where TVariableLengthInput : IVariableLengthInput<TInput>
                => allocator.GetUpsertRecordSize(key, valueObject, ref input, varlenInput);

            public static bool InitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.InitialWriter(ref logRecord, in sizeInfo, ref input, valueObject, ref output, ref upsertInfo);

            public static void PostInitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.PostInitialWriter(ref logRecord, in sizeInfo, ref input, valueObject, ref output, ref upsertInfo);

            public static bool InPlaceWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.InPlaceWriter(ref logRecord, in sizeInfo, ref input, valueObject, ref output, ref upsertInfo);

            public static void PostUpsertOperation<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper, TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions, TEpochAccessor epochAccessor)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                where TEpochAccessor : IEpochAccessor
                => sessionFunctions.PostUpsertOperation(key, ref input, valueObject, ref upsertInfo, epochAccessor);
        }

        internal struct LogRecordUpsertValueSelector : IUpsertValueSelector
        {
            public static RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(TAllocator allocator, ReadOnlySpan<byte> key,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
                where TSourceLogRecord : ISourceLogRecord
                where TVariableLengthInput : IVariableLengthInput<TInput>
                => allocator.GetUpsertRecordSize(key, in inputLogRecord, ref input, varlenInput);

            public static bool InitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.InitialWriter(ref logRecord, in sizeInfo, ref input, in inputLogRecord, ref output, ref upsertInfo);

            public static void PostInitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.PostInitialWriter(ref logRecord, in sizeInfo, ref input, in inputLogRecord, ref output, ref upsertInfo);

            public static bool InPlaceWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                => sessionFunctions.InPlaceWriter(ref logRecord, in sizeInfo, ref input, in inputLogRecord, ref output, ref upsertInfo);

            public static void PostUpsertOperation<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper, TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input,
                    ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, in TSourceLogRecord inputLogRecord, ref UpsertInfo upsertInfo, TSessionFunctionsWrapper sessionFunctions, TEpochAccessor epochAccessor)
                where TSourceLogRecord : ISourceLogRecord
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                where TEpochAccessor : IEpochAccessor
            {
                if (!inputLogRecord.Info.ValueIsObject)
                    sessionFunctions.PostUpsertOperation(key, ref input, inputLogRecord.ValueSpan, ref upsertInfo, epochAccessor);
                else
                    sessionFunctions.PostUpsertOperation(key, ref input, inputLogRecord.ValueObject, ref upsertInfo, epochAccessor);
            }
        }
    }
}