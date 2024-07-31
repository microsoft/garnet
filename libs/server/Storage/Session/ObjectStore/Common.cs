// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        #region Common ObjectStore Methods

        unsafe GarnetStatus RMWObjectStoreOperation<TObjectContext>(byte[] key, ref ObjectInput input,
            out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            output = new();
            var objStoreOutput = new GarnetObjectStoreOutput
            {
                spanByteAndMemory =
                    new(SpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size))
            };

            // Perform RMW on object store
            var status = objectStoreContext.RMW(ref key, ref input, ref objStoreOutput);

            return CompletePendingAndGetGarnetStatus(status, ref objectStoreContext, ref objStoreOutput);
        }

        unsafe GarnetStatus RMWObjectStoreOperation<TObjectContext>(byte[] key, ArgSlice input,
            out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        unsafe GarnetStatus RMWObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ref ObjectInput input,
            ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput outputFooter)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            // Perform RMW on object store
            var status = objectStoreContext.RMW(ref key, ref input, ref outputFooter);

            return CompletePendingAndGetGarnetStatus(status, ref objectStoreContext, ref outputFooter);
        }

        /// <summary>
        /// Perform Read operation in object store 
        /// use this method in commands that return an array
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ref ObjectInput input,
            ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput outputFooter)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            // Perform read on object store
            var status = objectStoreContext.Read(ref key, ref input, ref outputFooter);

            return CompletePendingAndGetGarnetStatus(status, ref objectStoreContext, ref outputFooter);
        }

        /// <summary>
        /// Perform Read operation in object store 
        /// use this method in commands that return an array
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ArgSlice input,
            ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput outputFooter)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            ref var objInput = ref Unsafe.AsRef<ObjectInput>(input.ptr);

            return ReadObjectStoreOperationWithOutput(key, ref objInput, ref objectStoreContext, ref outputFooter);
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
            int len = 0;

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

                        int i = 0;
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
        /// <returns>integer</returns>
        unsafe long ProcessRespSimple64IntOutput(GarnetObjectStoreOutput outputFooter)
        {
            long result;

            var outputSpan = outputFooter.spanByteAndMemory.IsSpanByte ?
                outputFooter.spanByteAndMemory.SpanByte.AsReadOnlySpan() : outputFooter.spanByteAndMemory.AsMemoryReadOnlySpan();
            try
            {
                fixed (byte* outputPtr = outputSpan)
                {
                    var refPtr = outputPtr;

                    if (!RespReadUtils.Read64Int(out result, ref refPtr, outputPtr + outputSpan.Length))
                        return default;
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
        /// Gets the value of the key store in the Object Store
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperation<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            ref var _input = ref Unsafe.AsRef<ObjectInput>(input.ptr);

            output = new();
            var _output = new GarnetObjectStoreOutput { spanByteAndMemory = new(SpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size)) };

            // Perform Read on object store
            var status = objectStoreContext.Read(ref key, ref _input, ref _output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref _output, ref objectStoreContext);

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
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperation<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            if (objectStoreContext.Session is null)
                ThrowObjectStoreUninitializedException();

            output = new();
            var _output = new GarnetObjectStoreOutput { spanByteAndMemory = new(SpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size)) };

            // Perform Read on object store
            var status = objectStoreContext.Read(ref key, ref input, ref _output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref _output, ref objectStoreContext);

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
        /// <param name="objectStoreContext"></param>
        public GarnetStatus ObjectScan<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
          => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowObjectStoreUninitializedException()
            => throw new GarnetException("Object store is disabled");

        #endregion

        /// <summary>
        /// Complete operation if pending and get GarnetStatus based on status returned from the Object Store
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="status"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        private GarnetStatus CompletePendingAndGetGarnetStatus<TObjectContext>(Status status, ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput outputFooter)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref outputFooter, ref objectStoreContext);

            if (status.NotFound && !status.Record.Created)
                return GarnetStatus.NOTFOUND;

            if (status.Found && outputFooter.spanByteAndMemory.Length == 0)
                return GarnetStatus.WRONGTYPE;

            return GarnetStatus.OK;
        }
    }
}