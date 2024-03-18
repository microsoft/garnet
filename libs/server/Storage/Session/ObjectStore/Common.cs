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

        unsafe GarnetStatus RMWObjectStoreOperation<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var _input = input.SpanByte;

            output = new();
            var _output = new GarnetObjectStoreOutput { spanByteAndMemory = new(SpanByte.FromPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size)) };

            // Perform RMW on object store
            var status = objectStoreContext.RMW(ref key, ref _input, ref _output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref _output, ref objectStoreContext);

            Debug.Assert(_output.spanByteAndMemory.IsSpanByte);


            if (!status.Record.Created && !status.Record.CopyUpdated && !status.Record.InPlaceUpdated)
                return GarnetStatus.NOTFOUND;

            return GarnetStatus.OK;
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
        GarnetStatus RMWObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ArgSlice input, ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput outputFooter)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var _input = input.SpanByte;

            // Perform RMW on object store
            var status = objectStoreContext.RMW(ref key, ref _input, ref outputFooter);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref outputFooter, ref objectStoreContext);

            if (!status.Record.Created && !status.Record.CopyUpdated && !status.Record.InPlaceUpdated)
                return GarnetStatus.NOTFOUND;

            return GarnetStatus.OK;
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
        GarnetStatus ReadObjectStoreOperationWithOutput<TObjectContext>(byte[] key, ArgSlice input, ref TObjectContext objectStoreContext, ref GarnetObjectStoreOutput outputFooter)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var _input = input.SpanByte;

            // Perform read on object store
            var status = objectStoreContext.Read(ref key, ref _input, ref outputFooter);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref outputFooter, ref objectStoreContext);

            if (status.NotFound)
                return GarnetStatus.NOTFOUND;

            return GarnetStatus.OK;
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
                            if (!RespReadUtils.ReadArrayLength(out var outerArraySize, ref refPtr, outputPtr + outputSpan.Length))
                                return default;

                            element = null;
                            len = 0;
                            // Read cursor value
                            if (!RespReadUtils.ReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                                return default;
                        }

                        // Get the number of elements
                        if (!RespReadUtils.ReadArrayLength(out var arraySize, ref refPtr, outputPtr + outputSpan.Length))
                            return default;

                        // Create the argslice[]
                        elements = new ArgSlice[isScanOutput ? arraySize + 1 : arraySize];

                        int i = 0;
                        if (isScanOutput)
                            elements[i++] = new ArgSlice { ptr = element, length = len };

                        for (; i < elements.Length; i++)
                        {
                            element = null;
                            len = 0;
                            if (RespReadUtils.ReadPtrWithLengthHeader(ref element, ref len, ref refPtr, outputPtr + outputSpan.Length))
                            {
                                elements[i] = new ArgSlice { ptr = element, length = len };
                            }
                        }
                    }
                    else
                    {
                        byte* result = null;
                        len = 0;
                        if (!RespReadUtils.ReadPtrWithLengthHeader(ref result, ref len, ref refPtr, outputPtr + outputSpan.Length))
                            return default;
                        elements = new ArgSlice[1] { new ArgSlice { ptr = result, length = len } };
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
        /// Gets the value of the key store in the Object Store
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        unsafe GarnetStatus ReadObjectStoreOperation<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var _input = input.SpanByte;

            output = new();
            var _output = new GarnetObjectStoreOutput { spanByteAndMemory = new(SpanByte.FromPointer((byte*)Unsafe.AsPointer(ref output), ObjectOutputHeader.Size)) };

            // Perform RMW on object store
            var status = objectStoreContext.Read(ref key, ref _input, ref _output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref _output, ref objectStoreContext);

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
        public GarnetStatus ObjectScan<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
          => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

        #endregion
    }
}