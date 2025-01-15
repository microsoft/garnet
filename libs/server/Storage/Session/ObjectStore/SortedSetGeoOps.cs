// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Adds the specified geospatial items (longitude, latitude, name) to the specified key.
        /// Data is stored into the key as a sorted set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus GeoAdd<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
          => RMWObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref outputFooter);

        /// <summary>
        /// GEOHASH: Returns valid Geohash strings representing the position of one or more elements in a geospatial data of the sorted set.
        /// GEODIST: Returns the distance between two members in the geospatial index represented by the sorted set.
        /// GEOPOS: Returns the positions (longitude,latitude) of all the specified members in the sorted set.
        /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus GeoCommands<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Geospatial search and store in destination key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="destination"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus GeoSearchStore<TObjectContext>(ArgSlice key, ArgSlice destination, ref ObjectInput input, ref SpanByteAndMemory output, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(destination, true, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(key, true, LockType.Shared);
                _ = txnManager.Run(true);
            }
            var objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();
            var curr = ptr;
            var end = curr + output.Length;

            try
            {
                var sourceKey = key.ToArray();
                SpanByteAndMemory searchOutMem = default;
                var searchOut = new GarnetObjectStoreOutput { spanByteAndMemory = searchOutMem };
                var status = GeoCommands(sourceKey, ref input, ref searchOut, ref objectStoreLockableContext);
                searchOutMem = searchOut.spanByteAndMemory;

                if (status == GarnetStatus.WRONGTYPE)
                {
                    return GarnetStatus.WRONGTYPE;
                }

                if (status == GarnetStatus.NOTFOUND)
                {
                    // Expire/Delete the destination key if the source key is not found
                    _ = EXPIRE(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None, ref lockableContext, ref objectStoreLockableContext);
                    while (!RespWriteUtils.WriteInteger(0, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return GarnetStatus.OK;
                }

                Debug.Assert(!searchOutMem.IsSpanByte, "Output should not be in SpanByte format when the status is OK");

                var searchOutHandler = searchOutMem.Memory.Memory.Pin();
                try
                {
                    var searchOutPtr = (byte*)searchOutHandler.Pointer;
                    ref var currOutPtr = ref searchOutPtr;
                    var endOutPtr = searchOutPtr + searchOutMem.Length;

                    if (RespReadUtils.TryReadErrorAsSpan(out var error, ref currOutPtr, endOutPtr))
                    {
                        while (!RespWriteUtils.WriteError(error, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return GarnetStatus.OK;
                    }

                    var destinationKey = destination.ToArray();
                    objectStoreLockableContext.Delete(ref destinationKey);

                    RespReadUtils.ReadUnsignedArrayLength(out var foundItems, ref currOutPtr, endOutPtr);

                    // Prepare the parse state for sorted set add
                    parseState.Initialize(foundItems * 2);

                    for (var j = 0; j < foundItems; j++)
                    {
                        RespReadUtils.ReadUnsignedArrayLength(out var innerLength, ref currOutPtr, endOutPtr);
                        Debug.Assert(innerLength == 2, "Should always has location and hash or distance");

                        // Read location into parse state
                        parseState.Read((2 * j) + 1, ref currOutPtr, endOutPtr);
                        // Read score into parse state
                        parseState.Read(2 * j, ref currOutPtr, endOutPtr);
                    }

                    // Prepare the input
                    var zAddInput = new ObjectInput(new RespInputHeader
                    {
                        type = GarnetObjectType.SortedSet,
                        SortedSetOp = SortedSetOperation.ZADD,
                    }, ref parseState);

                    var zAddOutput = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
                    RMWObjectStoreOperationWithOutput(destinationKey, ref zAddInput, ref objectStoreLockableContext, ref zAddOutput);

                    while (!RespWriteUtils.WriteInteger(foundItems, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                finally
                {
                    searchOutHandler.Dispose();
                }

                return GarnetStatus.OK;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}