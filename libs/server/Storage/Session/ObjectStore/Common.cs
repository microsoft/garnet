// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.common.Parsing;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    sealed partial class StorageSession : IDisposable
    {
        #region Common ObjectStore Methods

        unsafe GarnetStatus RMWObjectStoreOperation<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            var objStoreOutput = new GarnetObjectStoreOutput();

            // Perform RMW on object store
            var status = objectStoreContext.RMW(ref key, ref input, ref objStoreOutput);

            output = objStoreOutput.Header;

            return CompletePendingAndGetGarnetStatus(status, ref objectStoreContext, ref objStoreOutput);
        }

        unsafe GarnetStatus RMWObjectStoreOperation<TObjectContext>(byte[] key, ArgSlice input,
            out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            ref var objInput = ref Unsafe.AsRef<ObjectInput>(input.ptr);

            return RMWObjectStoreOperation(key, ref objInput, out output, ref objectStoreContext);
        }

        /// <summary>
        /// Perform RMW operation in object store
        /// use this method in commands that return an array
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus RMWObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ref ObjectInput input, ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput output)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            // Perform RMW on object store
            var status = objectStoreContext.RMW(ref key, ref input, ref output);

            return CompletePendingAndGetGarnetStatus(status, ref objectStoreContext, ref output);
        }

        /// <summary>
        /// Perform Read operation in object store
        /// use this method in commands that return an array
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ReadObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ref ObjectInput input, ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput output)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            // Perform read on object store
            var status = objectStoreContext.Read(ref key, ref input, ref output);

            return CompletePendingAndGetGarnetStatus(status, ref objectStoreContext, ref output);
        }

        /// <summary>
        /// Perform Read operation in object store
        /// use this method in commands that return an array
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ArgSlice input,
            ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput output)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            ref var objInput = ref Unsafe.AsRef<ObjectInput>(input.ptr);

            return ReadObjectStoreOperationWithOutput(key, ref objInput, ref objectStoreContext, ref output);
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
        /// <param name="objectStoreContext"></param>
        public unsafe GarnetStatus ObjectScan<TObjectContext>(GarnetObjectType objectType, ArgSlice key, long cursor, string match, int count, out ArgSlice[] items, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            Debug.Assert(objectType is GarnetObjectType.Hash or GarnetObjectType.Set or GarnetObjectType.SortedSet);

            items = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            if (string.IsNullOrEmpty(match))
                match = "*";

            // Prepare the parse state
            var matchPattern = match.Trim();

            var cursorLength = NumUtils.CountDigits(cursor);
            var countLength = NumUtils.CountDigits(count);

            // Calculate # of bytes to store parameters
            var sliceBytes = cursorLength +
                             CmdStrings.MATCH.Length +
                             matchPattern.Length +
                             CmdStrings.COUNT.Length +
                             countLength;

            // Get buffer from scratch buffer manager
            var paramsSlice = scratchBufferManager.CreateArgSlice(sliceBytes);
            var paramsSpan = paramsSlice.Span;
            var paramsSpanOffset = 0;

            // Store parameters in buffer
            // cursor
            var cursorSpan = paramsSpan.Slice(paramsSpanOffset, cursorLength);
            NumUtils.WriteInt64(cursor, cursorSpan);
            paramsSpanOffset += cursorLength;
            var cursorSlice = ArgSlice.FromPinnedSpan(cursorSpan);

            // MATCH
            var matchSpan = paramsSpan.Slice(paramsSpanOffset, CmdStrings.MATCH.Length);
            CmdStrings.MATCH.CopyTo(matchSpan);
            paramsSpanOffset += CmdStrings.MATCH.Length;
            var matchSlice = ArgSlice.FromPinnedSpan(matchSpan);

            // Pattern
            var patternSpan = paramsSpan.Slice(paramsSpanOffset, matchPattern.Length);
            Encoding.ASCII.GetBytes(matchPattern, patternSpan);
            paramsSpanOffset += matchPattern.Length;
            var matchPatternSlice = ArgSlice.FromPinnedSpan(patternSpan);

            // COUNT
            var countSpan = paramsSpan.Slice(paramsSpanOffset, CmdStrings.COUNT.Length);
            CmdStrings.COUNT.CopyTo(countSpan);
            paramsSpanOffset += CmdStrings.COUNT.Length;
            var countSlice = ArgSlice.FromPinnedSpan(countSpan);

            // Value
            var countValueSpan = paramsSpan.Slice(paramsSpanOffset, countLength);
            NumUtils.WriteInt64(count, countValueSpan);
            var countValueSlice = ArgSlice.FromPinnedSpan(countValueSpan);

            parseState.InitializeWithArguments(cursorSlice, matchSlice, matchPatternSlice,
                countSlice, countValueSlice);

            // Prepare the input
            var header = new RespInputHeader(objectType);
            var input = new ObjectInput(header, ref parseState, arg2: ObjectScanCountLimit);

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

            var output = new GarnetObjectStoreOutput();
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref output);

            scratchBufferManager.RewindScratchBuffer(ref paramsSlice);

            items = default;
            if (status == GarnetStatus.OK)
                items = ProcessRespArrayOutput(output, out _, isScanOutput: true);

            return status;

        }

        /// <summary>
        /// Converts an array of elements in RESP format to ArgSlice[] type
        /// </summary>
        /// <param name="output">The RESP format output object</param>
        /// <param name="error">A description of the error, if there is any</param>
        /// <param name="isScanOutput">True when the output comes from HSCAN, ZSCAN OR SSCAN command</param>
        /// <returns></returns>
        unsafe ArgSlice[] ProcessRespArrayOutput(GarnetObjectStoreOutput output, out string error, bool isScanOutput = false)
        {
            ArgSlice[] elements = default;
            error = default;

            // For reading the elements in the output
            byte* element = null;
            int len = 0;

            var outputSpan = output.SpanByteAndMemory.IsSpanByte ?
                             output.SpanByteAndMemory.SpanByte.AsReadOnlySpan() : output.SpanByteAndMemory.AsMemoryReadOnlySpan();

            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (*refPtr == '-')
                    {
                        if (!RespReadUtils.TryReadErrorAsString(out error, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                    }
                    else if (*refPtr == '*')
                    {
                        if (isScanOutput)
                        {
                            // Read the first two elements
                            if (!RespReadUtils.TryReadUnsignedArrayLength(out var outerArraySize, ref refPtr, outputPtr + outputSpan.Length))
                                return default;

                            element = null;
                            len = 0;
                            // Read cursor value
                            if (!RespReadUtils.TryReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                                return default;
                        }

                        // Get the number of elements
                        if (!RespReadUtils.TryReadUnsignedArrayLength(out var arraySize, ref refPtr, outputPtr + outputSpan.Length))
                            return default;

                        // Create the argslice[]
                        elements = new ArgSlice[isScanOutput ? arraySize + 1 : arraySize];

                        int i = 0;
                        if (isScanOutput)
                            elements[i++] = new ArgSlice(element, len);

                        for (; i < elements.Length; i++)
                        {
                            element = null;
                            len = 0;
                            if (RespReadUtils.TryReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                            {
                                elements[i] = new ArgSlice(element, len);
                            }
                        }
                    }
                    else
                    {
                        byte* result = null;
                        len = 0;
                        if (!RespReadUtils.TryReadPtrWithLengthHeader(ref result, ref len, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                        elements = [new ArgSlice(result, len)];
                    }
                }
            }
            finally
            {
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Memory.Dispose();
            }

            return elements;
        }

        /// <summary>
        /// Converts an array of elements in RESP format to ArgSlice[] type
        /// </summary>
        /// <param name="output">The RESP format output object</param>
        /// <param name="error">A description of the error, if there is any</param>
        /// <returns></returns>
        unsafe int[] ProcessRespIntegerArrayOutput(GarnetObjectStoreOutput output, out string error)
        {
            int[] elements = default;
            error = default;

            // For reading the elements in the output
            byte* element = null;

            var outputSpan = output.SpanByteAndMemory.IsSpanByte ?
                             output.SpanByteAndMemory.SpanByte.AsReadOnlySpan() : output.SpanByteAndMemory.AsMemoryReadOnlySpan();

            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (*refPtr == '-')
                    {
                        if (!RespReadUtils.TryReadErrorAsString(out error, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                    }
                    else if (*refPtr == '*')
                    {
                        // Get the number of elements
                        if (!RespReadUtils.TryReadUnsignedArrayLength(out var arraySize, ref refPtr, outputPtr + outputSpan.Length))
                            return default;

                        // Create the argslice[]
                        elements = new int[arraySize];
                        for (int i = 0; i < elements.Length; i++)
                        {
                            if (*refPtr != ':')
                            {
                                RespParsingException.ThrowUnexpectedToken(*refPtr);
                            }
                            refPtr++;

                            element = null;
                            if (RespReadUtils.TryReadInt32(ref refPtr, outputPtr + outputSpan.Length, out var number, out var _))
                            {
                                elements[i] = number;
                            }

                            if (*(ushort*)refPtr != MemoryMarshal.Read<ushort>("\r\n"u8))
                            {
                                RespParsingException.ThrowUnexpectedToken(*refPtr);
                            }

                            refPtr += 2;
                        }
                    }
                }
            }
            finally
            {
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Memory.Dispose();
            }

            return elements;
        }

        /// <summary>
        /// Converts an array of elements in RESP format to ArgSlice[] type
        /// </summary>
        /// <param name="output">The RESP format output object</param>
        /// <param name="error">A description of the error, if there is any</param>
        /// <returns></returns>
        unsafe long[] ProcessRespInt64ArrayOutput(GarnetObjectStoreOutput output, out string error)
        {
            long[] elements = default;
            error = default;

            // For reading the elements in the output
            byte* element = null;

            var outputSpan = output.SpanByteAndMemory.IsSpanByte ?
                             output.SpanByteAndMemory.SpanByte.AsReadOnlySpan() : output.SpanByteAndMemory.AsMemoryReadOnlySpan();

            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (*refPtr == '-')
                    {
                        if (!RespReadUtils.TryReadErrorAsString(out error, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                    }
                    else if (*refPtr == '*')
                    {
                        // Get the number of elements
                        if (!RespReadUtils.TryReadUnsignedArrayLength(out var arraySize, ref refPtr, outputPtr + outputSpan.Length))
                            return default;

                        // Create the argslice[]
                        elements = new long[arraySize];
                        for (int i = 0; i < elements.Length; i++)
                        {
                            if (*refPtr != ':')
                            {
                                RespParsingException.ThrowUnexpectedToken(*refPtr);
                            }
                            refPtr++;

                            element = null;
                            if (RespReadUtils.TryReadInt64(ref refPtr, outputPtr + outputSpan.Length, out var number, out var _))
                            {
                                elements[i] = number;
                            }

                            if (*(ushort*)refPtr != MemoryMarshal.Read<ushort>("\r\n"u8))
                            {
                                RespParsingException.ThrowUnexpectedToken(*refPtr);
                            }

                            refPtr += 2;
                        }
                    }
                }
            }
            finally
            {
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Memory.Dispose();
            }

            return elements;
        }

        /// <summary>
        /// Processes RESP output as pairs of score and member.
        /// </summary>
        unsafe (ArgSlice member, ArgSlice score)[] ProcessRespArrayOutputAsPairs(GarnetObjectStoreOutput output, out string error)
        {
            (ArgSlice member, ArgSlice score)[] result = default;
            error = default;
            byte* element = null;
            var len = 0;
            var outputSpan = output.SpanByteAndMemory.IsSpanByte ?
                             output.SpanByteAndMemory.SpanByte.AsReadOnlySpan() : output.SpanByteAndMemory.AsMemoryReadOnlySpan();

            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (*refPtr == '-')
                    {
                        if (!RespReadUtils.TryReadErrorAsString(out error, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                    }
                    else if (*refPtr == '*')
                    {
                        // Get the number of result elements
                        if (!RespReadUtils.TryReadUnsignedArrayLength(out var arraySize, ref refPtr, outputPtr + outputSpan.Length))
                            return default;

                        Debug.Assert(arraySize % 2 == 0, "Array elements are expected to be in pairs");
                        arraySize /= 2; // Halve the array size to hold items as pairs
                        result = new (ArgSlice member, ArgSlice score)[arraySize];

                        for (var i = 0; i < result.Length; i++)
                        {
                            if (!RespReadUtils.TryReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                                return default;

                            result[i].member = new ArgSlice(element, len);

                            if (!RespReadUtils.TryReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                                return default;

                            result[i].score = new ArgSlice(element, len);
                        }
                    }
                }
            }
            finally
            {
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Memory.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Converts a single token in RESP format to ArgSlice type
        /// </summary>
        /// <param name="output">The RESP format output object</param>
        /// <returns></returns>
        unsafe ArgSlice ProcessRespSingleTokenOutput(GarnetObjectStoreOutput output)
        {
            byte* element = null;
            var len = 0;
            ArgSlice result;

            var outputSpan = output.SpanByteAndMemory.IsSpanByte ?
                             output.SpanByteAndMemory.SpanByte.AsReadOnlySpan() : output.SpanByteAndMemory.AsMemoryReadOnlySpan();
            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (!RespReadUtils.TryReadPtrWithLengthHeader(ref element, ref len, ref refPtr,
                            outputPtr + outputSpan.Length))
                        return default;
                    result = new ArgSlice(element, len);
                }
            }
            finally
            {
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Memory.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Converts a simple integer in RESP format to integer type
        /// </summary>
        /// <param name="output">The RESP format output object</param>
        /// <param name="value"></param>
        /// <returns>integer</returns>
        unsafe bool TryProcessRespSimple64IntOutput(GarnetObjectStoreOutput output, out long value)
        {
            var outputSpan = output.SpanByteAndMemory.IsSpanByte ?
                output.SpanByteAndMemory.SpanByte.AsReadOnlySpan() : output.SpanByteAndMemory.AsMemoryReadOnlySpan();
            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (!RespReadUtils.TryReadInt64(out value, ref refPtr, outputPtr + outputSpan.Length, out _))
                        return false;
                }
            }
            finally
            {
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Memory.Dispose();
            }

            return true;
        }

        /// <summary>
        /// Gets the value of the key store in the Object Store
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperation<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            ref var _input = ref Unsafe.AsRef<ObjectInput>(input.ptr);

            var _output = new GarnetObjectStoreOutput();

            // Perform Read on object store
            var status = objectStoreContext.Read(ref key, ref _input, ref _output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref _output, ref objectStoreContext);

            output = _output.Header;

            if (_output.HasWrongType)
                return GarnetStatus.WRONGTYPE;

            if (status.Found && (!status.Record.Created && !status.Record.CopyUpdated && !status.Record.InPlaceUpdated))
                return GarnetStatus.OK;

            return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Gets the value of the key store in the Object Store
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperation<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            var _output = new GarnetObjectStoreOutput();

            // Perform Read on object store
            var status = objectStoreContext.Read(ref key, ref input, ref _output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref _output, ref objectStoreContext);

            output = _output.Header;

            if (_output.HasWrongType)
                return GarnetStatus.WRONGTYPE;

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
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        public GarnetStatus ObjectScan<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
          => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowObjectStoreUninitializedException()
            => throw new GarnetException("Object store is disabled", disposeSession: false);

        #endregion

        /// <summary>
        /// Complete operation if pending and get GarnetStatus based on status returned from the Object Store
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="status"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        private GarnetStatus CompletePendingAndGetGarnetStatus<TObjectContext>(Status status, ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput output)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectStoreContext);

            if (status.NotFound && !status.Record.Created)
                return GarnetStatus.NOTFOUND;

            if (status.Found && output.HasWrongType)
                return GarnetStatus.WRONGTYPE;

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Collects objects from the object store based on the specified input and type.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="searchKey">The key to search for in the object store.</param>
        /// <param name="typeObject">The type of the object to collect.</param>
        /// <param name="collectLock">The lock to ensure single writer and multiple readers.</param>
        /// <param name="input">The input object for the operation.</param>
        /// <param name="objectContext">The context of the object store.</param>
        /// <returns>The status of the operation.</returns>
        private GarnetStatus ObjectCollect<TObjectContext>(ArgSlice searchKey, ReadOnlySpan<byte> typeObject, SingleWriterMultiReaderLock collectLock, ref ObjectInput input, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (!collectLock.TryWriteLock())
            {
                return GarnetStatus.NOTFOUND;
            }

            try
            {
                long cursor = 0;
                long storeCursor = 0;

                do
                {
                    if (!DbScan(searchKey, true, cursor, out storeCursor, out var hashKeys, 100, typeObject))
                    {
                        return GarnetStatus.OK;
                    }

                    foreach (var hashKey in hashKeys)
                    {
                        RMWObjectStoreOperation(hashKey, ref input, out _, ref objectContext);
                    }

                    cursor = storeCursor;
                } while (storeCursor != 0);

                return GarnetStatus.OK;
            }
            finally
            {
                collectLock.WriteUnlock();
            }
        }
    }
}