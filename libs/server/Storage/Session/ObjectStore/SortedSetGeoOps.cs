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
                var storeDistIdx = -1;
                var i = 0;
                while (i < input.parseState.Count)
                {
                    if (input.parseState.GetArgSliceByRef(i).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STOREDIST))
                    {
                        storeDistIdx = i;
                        break;
                    }
                    i++;
                }

                SessionParseState parseState = default;
                // Prepare the arguments for geo search when store distance is not provided
                if (storeDistIdx == -1)
                {
                    parseState = new SessionParseState();
                    parseState.Initialize(input.parseState.Count + 1);
                    parseState.SetArguments(0, input.parseState.Parameters);
                    parseState.SetArguments(input.parseState.Count, ArgSlice.FromPinnedSpan(CmdStrings.WITHHASH));
                }
                // When StoreDist is provided as the last argument
                else if (storeDistIdx == input.parseState.Count - 1)
                {
                    parseState = new SessionParseState();
                    parseState.Initialize(input.parseState.Count);
                    parseState.SetArguments(0, input.parseState.Parameters.Slice(0, input.parseState.Count - 1));
                    parseState.SetArguments(input.parseState.Count - 1, ArgSlice.FromPinnedSpan(CmdStrings.WITHDIST));
                }
                // When StoreDist is provided in the middle
                else
                {
                    parseState = new SessionParseState();
                    parseState.Initialize(input.parseState.Count);
                    parseState.SetArguments(0, input.parseState.Parameters.Slice(0, storeDistIdx));
                    parseState.SetArguments(storeDistIdx, ArgSlice.FromPinnedSpan(CmdStrings.WITHDIST));
                    parseState.SetArguments(storeDistIdx + 1, input.parseState.Parameters.Slice(storeDistIdx + 1));
                }

                var sourceKey = key.ToArray();
                var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.GEOSEARCH };
                var searchInput = new ObjectInput(header, ref parseState);

                SpanByteAndMemory searchOutMem = default;
                var searchOut = new GarnetObjectStoreOutput { spanByteAndMemory = searchOutMem };
                var status = GeoCommands(sourceKey, ref searchInput, ref searchOut, ref objectStoreLockableContext);
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
                    var currOutPtr = searchOutPtr;
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
                    var zParseState = new SessionParseState();
                    zParseState.Initialize(foundItems * 2);

                    for (int j = 0; j < foundItems; j++)
                    {
                        RespReadUtils.ReadUnsignedArrayLength(out var innerLength, ref currOutPtr, endOutPtr);
                        Debug.Assert(innerLength == 2, "Should always has location and hash or distance");

                        RespReadUtils.TrySliceWithLengthHeader(out var location, ref currOutPtr, endOutPtr);
                        if (storeDistIdx != -1)
                        {
                            RespReadUtils.ReadSpanWithLengthHeader(out var score, ref currOutPtr, endOutPtr);
                            zParseState.SetArgument(2 * j, ArgSlice.FromPinnedSpan(score));
                            zParseState.SetArgument((2 * j) + 1, ArgSlice.FromPinnedSpan(location));
                        }
                        else
                        {
                            RespReadUtils.ReadIntegerAsSpan(out var score, ref currOutPtr, endOutPtr);
                            zParseState.SetArgument(2 * j, ArgSlice.FromPinnedSpan(score));
                            zParseState.SetArgument((2 * j) + 1, ArgSlice.FromPinnedSpan(location));
                        }
                    }

                    // Prepare the input
                    var zAddInput = new ObjectInput(new RespInputHeader
                    {
                        type = GarnetObjectType.SortedSet,
                        SortedSetOp = SortedSetOperation.ZADD,
                    }, ref zParseState);

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