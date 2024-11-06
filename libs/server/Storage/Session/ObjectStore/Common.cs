// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        #region Common ObjectStore Methods

        unsafe GarnetStatus RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (!dualContext.IsDual)
                ThrowObjectStoreUninitializedException();

            output = new();
            var objStoreOutput = new GarnetObjectStoreOutput
            {
                spanByteAndMemory =
                    new(SpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size))
            };

            // Perform RMW on object store
            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref objStoreOutput);
            return CompletePendingAndGetGarnetStatus<TKeyLocker>(status, ref objStoreOutput);
        }

        unsafe GarnetStatus RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (!dualContext.IsDual)
                ThrowObjectStoreUninitializedException();

            ref var objInput = ref Unsafe.AsRef<ObjectInput>(input.ptr);
            return RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref objInput, out output);
        }

        /// <summary>
        /// Perform RMW operation in object store 
        /// use this method in commands that return an array
        /// </summary>
        GarnetStatus RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (!dualContext.IsDual)
                ThrowObjectStoreUninitializedException();

            // Perform RMW on object store
            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref outputFooter);
            return CompletePendingAndGetGarnetStatus<TKeyLocker>(status, ref outputFooter);
        }

        /// <summary>
        /// Perform Read operation in object store 
        /// use this method in commands that return an array
        /// </summary>
        GarnetStatus ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (!dualContext.IsDual)
                ThrowObjectStoreUninitializedException();

            // Perform read on object store
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref outputFooter);
            return CompletePendingAndGetGarnetStatus<TKeyLocker>(status, ref outputFooter);
        }

        /// <summary>
        /// Perform Read operation in object store 
        /// use this method in commands that return an array
        /// </summary>
        unsafe GarnetStatus ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (!dualContext.IsDual)
                ThrowObjectStoreUninitializedException();

            ref var objInput = ref Unsafe.AsRef<ObjectInput>(input.ptr);
            return ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref objInput, ref outputFooter);
        }

        /// <summary>
        /// Common functionality for executing SSCAN, HSCAN and ZSCAN
        /// </summary>
        /// <param name="objectType"></param>
        /// <param name="key">The key of the object</param>
        /// <param name="cursor">The value of the cursor</param>
        /// <param name="match">The pattern to match</param>
        /// <param name="count">Limit number for the response</param>
        /// <param name="items">The list of items for the response</param>
        public unsafe GarnetStatus ObjectScan<TKeyLocker, TEpochGuard>(GarnetObjectType objectType, ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            Debug.Assert(objectType is GarnetObjectType.Hash or GarnetObjectType.Set or GarnetObjectType.SortedSet);

            items = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            if (string.IsNullOrEmpty(match))
                match = "*";

            // Prepare the parse state 
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;

            var matchPatternValue = Encoding.ASCII.GetBytes(match.Trim());

            var lengthCountNumber = NumUtils.NumDigits(count);
            var countBytes = new byte[lengthCountNumber];

            fixed (byte* matchKeywordPtr = CmdStrings.MATCH, matchPatterPtr = matchPatternValue)
            {
                fixed (byte* countPtr = CmdStrings.COUNT, countValuePtr = countBytes)
                {
                    var matchKeywordSlice = new ArgSlice(matchKeywordPtr, CmdStrings.MATCH.Length);
                    var matchPatternSlice = new ArgSlice(matchPatterPtr, matchPatternValue.Length);

                    var countValuePtr2 = countValuePtr;
                    NumUtils.IntToBytes(count, lengthCountNumber, ref countValuePtr2);

                    var countKeywordSlice = new ArgSlice(countPtr, CmdStrings.COUNT.Length);
                    var countValueSlice = new ArgSlice(countValuePtr, countBytes.Length);

                    parseState.InitializeWithArguments(ref parseStateBuffer, matchKeywordSlice, matchPatternSlice,
                        countKeywordSlice, countValueSlice);
                }
            }

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = objectType,
                },
                arg1 = (int)cursor,
                arg2 = ObjectScanCountLimit,
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            switch (objectType)
            {
                case GarnetObjectType.Set:
                    input.header.SetOp = SetOperation.SSCAN;
                    break;
                case GarnetObjectType.Hash:
                    input.header.HashOp = HashOperation.HSCAN;
                    break;
                case GarnetObjectType.SortedSet:
                    input.header.SortedSetOp = SortedSetOperation.ZSCAN;
                    break;
            }

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);

            items = default;
            if (status == GarnetStatus.OK)
                items = ProcessRespArrayOutput(outputFooter, out _, isScanOutput: true);

            return status;

        }

        /// <summary>
        /// Converts an array of elements in RESP format to ArgSlice[] type
        /// </summary>
        /// <param name="outputFooter">The RESP format output object</param>
        /// <param name="error">A description of the error, if there is any</param>
        /// <param name="isScanOutput">True when the output comes from HSCAN, ZSCAN OR SSCAN command</param>
        /// <returns></returns>
        unsafe ArgSlice[] ProcessRespArrayOutput(GarnetObjectStoreOutput outputFooter, out string error, bool isScanOutput = false)
        {
            ArgSlice[] elements = default;
            error = default;

            // For reading the elements in the outputFooter
            byte* element = null;
            var len = 0;

            var outputSpan = outputFooter.spanByteAndMemory.IsSpanByte
                            ? outputFooter.spanByteAndMemory.SpanByte.AsReadOnlySpan()
                            : outputFooter.spanByteAndMemory.AsMemoryReadOnlySpan();

            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (*refPtr == '-')
                    {
                        if (!RespReadUtils.ReadErrorAsString(out error, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                    }
                    else if (*refPtr == '*')
                    {
                        if (isScanOutput)
                        {
                            // Read the first two elements
                            if (!RespReadUtils.ReadUnsignedArrayLength(out var outerArraySize, ref refPtr, outputPtr + outputSpan.Length))
                                return default;

                            element = null;
                            len = 0;
                            // Read cursor value
                            if (!RespReadUtils.ReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                                return default;
                        }

                        // Get the number of elements
                        if (!RespReadUtils.ReadUnsignedArrayLength(out var arraySize, ref refPtr, outputPtr + outputSpan.Length))
                            return default;

                        // Create the argslice[]
                        elements = new ArgSlice[isScanOutput ? arraySize + 1 : arraySize];

                        var i = 0;
                        if (isScanOutput)
                            elements[i++] = new ArgSlice(element, len);

                        for (; i < elements.Length; i++)
                        {
                            element = null;
                            len = 0;
                            if (RespReadUtils.ReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                            {
                                elements[i] = new ArgSlice(element, len);
                            }
                        }
                    }
                    else
                    {
                        byte* result = null;
                        len = 0;
                        if (!RespReadUtils.ReadPtrWithLengthHeader(ref result, ref len, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                        elements = [new ArgSlice(result, len)];
                    }
                }
            }
            finally
            {
                if (!outputFooter.spanByteAndMemory.IsSpanByte)
                    outputFooter.spanByteAndMemory.Memory.Dispose();
            }

            return elements;
        }

        /// <summary>
        /// Processes RESP output as pairs of score and member. 
        /// </summary>
        unsafe (ArgSlice member, ArgSlice score)[] ProcessRespArrayOutputAsPairs(GarnetObjectStoreOutput outputFooter, out string error)
        {
            (ArgSlice member, ArgSlice score)[] result = default;
            error = default;
            byte* element = null;
            var len = 0;
            var outputSpan = outputFooter.spanByteAndMemory.IsSpanByte ?
                             outputFooter.spanByteAndMemory.SpanByte.AsReadOnlySpan() : outputFooter.spanByteAndMemory.AsMemoryReadOnlySpan();

            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (*refPtr == '-')
                    {
                        if (!RespReadUtils.ReadErrorAsString(out error, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                    }
                    else if (*refPtr == '*')
                    {
                        // Get the number of result elements
                        if (!RespReadUtils.ReadUnsignedArrayLength(out var arraySize, ref refPtr, outputPtr + outputSpan.Length))
                            return default;

                        Debug.Assert(arraySize % 2 == 0, "Array elements are expected to be in pairs");
                        arraySize /= 2; // Halve the array size to hold items as pairs
                        result = new (ArgSlice member, ArgSlice score)[arraySize];

                        for (var i = 0; i < result.Length; i++)
                        {
                            if (!RespReadUtils.ReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                                return default;

                            result[i].member = new ArgSlice(element, len);

                            if (!RespReadUtils.ReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                                return default;

                            result[i].score = new ArgSlice(element, len);
                        }
                    }
                }
            }
            finally
            {
                if (!outputFooter.spanByteAndMemory.IsSpanByte)
                    outputFooter.spanByteAndMemory.Memory.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Converts a single token in RESP format to ArgSlice type
        /// </summary>
        /// <param name="outputFooter">The RESP format output object</param>
        /// <returns></returns>
        unsafe ArgSlice ProcessRespSingleTokenOutput(GarnetObjectStoreOutput outputFooter)
        {
            byte* element = null;
            var len = 0;
            ArgSlice result;

            var outputSpan = outputFooter.spanByteAndMemory.IsSpanByte ?
                             outputFooter.spanByteAndMemory.SpanByte.AsReadOnlySpan() : outputFooter.spanByteAndMemory.AsMemoryReadOnlySpan();
            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (!RespReadUtils.ReadPtrWithLengthHeader(ref element, ref len, ref refPtr,
                            outputPtr + outputSpan.Length))
                        return default;
                    result = new ArgSlice(element, len);
                }
            }
            finally
            {
                if (!outputFooter.spanByteAndMemory.IsSpanByte)
                    outputFooter.spanByteAndMemory.Memory.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Converts a simple integer in RESP format to integer type
        /// </summary>
        /// <param name="outputFooter">The RESP format output object</param>
        /// <param name="value"></param>
        /// <returns>integer</returns>
        unsafe bool TryProcessRespSimple64IntOutput(GarnetObjectStoreOutput outputFooter, out long value)
        {
            var outputSpan = outputFooter.spanByteAndMemory.IsSpanByte ?
                outputFooter.spanByteAndMemory.SpanByte.AsReadOnlySpan() : outputFooter.spanByteAndMemory.AsMemoryReadOnlySpan();
            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (!RespReadUtils.TryRead64Int(out value, ref refPtr, outputPtr + outputSpan.Length, out _))
                        return false;
                }
            }
            finally
            {
                if (!outputFooter.spanByteAndMemory.IsSpanByte)
                    outputFooter.spanByteAndMemory.Memory.Dispose();
            }

            return true;
        }

        /// <summary>
        /// Gets the value of the key store in the Object Store
        /// </summary>
        unsafe GarnetStatus ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (!dualContext.IsDual)
                ThrowObjectStoreUninitializedException();

            ref var _input = ref Unsafe.AsRef<ObjectInput>(input.ptr);

            output = new();
            var _output = new GarnetObjectStoreOutput { spanByteAndMemory = new(SpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size)) };

            // Perform Read on object store
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref _input, ref _output);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out _output);

            if (_output.spanByteAndMemory.Length == 0)
                return GarnetStatus.WRONGTYPE;
            Debug.Assert(_output.spanByteAndMemory.IsSpanByte);

            if (status.Found && (!status.Record.Created && !status.Record.CopyUpdated && !status.Record.InPlaceUpdated))
                return GarnetStatus.OK;
            return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Gets the value of the key store in the Object Store
        /// </summary>
        unsafe GarnetStatus ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (!dualContext.IsDual)
                ThrowObjectStoreUninitializedException();

            output = new();
            var _output = new GarnetObjectStoreOutput { spanByteAndMemory = new(SpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size)) };

            // Perform Read on object store
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref _output);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out _output);

            if (_output.spanByteAndMemory.Length == 0)
                return GarnetStatus.WRONGTYPE;
            Debug.Assert(_output.spanByteAndMemory.IsSpanByte);

            if (status.Found && (!status.Record.Created && !status.Record.CopyUpdated && !status.Record.InPlaceUpdated))
                return GarnetStatus.OK;

            return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Iterates members of a collection object using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key">The key of the sorted set</param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        public GarnetStatus ObjectScan<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
          => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowObjectStoreUninitializedException()
            => throw new GarnetException("Object store is disabled");

        #endregion

        /// <summary>
        /// Complete operation if pending and get GarnetStatus based on status returned from the Object Store
        /// </summary>
        private GarnetStatus CompletePendingAndGetGarnetStatus<TKeyLocker>(Status status, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
        {
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out outputFooter);

            if (status.NotFound && !status.Record.Created)
                return GarnetStatus.NOTFOUND;

            if (status.Found && outputFooter.spanByteAndMemory.Length == 0)
                return GarnetStatus.WRONGTYPE;

            return GarnetStatus.OK;
        }
    }
}