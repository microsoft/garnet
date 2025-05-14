// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus GeoAdd<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
          => RMWObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// GEOHASH: Returns valid Geohash strings representing the position of one or more elements in a geospatial data of the sorted set.
        /// GEODIST: Returns the distance between two members in the geospatial index represented by the sorted set.
        /// GEOPOS: Returns the positions (longitude,latitude) of all the specified members in the sorted set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus GeoCommands<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// Geospatial search and return result..
        /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// GEORADIUS (read variant): Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center and radius.
        /// GEORADIUS_RO: Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center and radius.
        /// GEORADIUSBYMEMBER (read variant): Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center (derived from member) and radius.
        /// GEORADIUSBYMEMBER_RO: Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center (derived from member) and radius.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="opts"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus GeoSearchReadOnly<TObjectContext>(ArgSlice key, ref GeoSearchOptions opts,
                                                      ref ObjectInput input,
                                                      ref SpanByteAndMemory output,
                                                      ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(key, true, LockType.Shared);
                txnManager.Run(true);
            }

            try
            {
                // Can we optimize more when ANY is used?
                var statusOp = GET(key.ToArray(), out var firstObj, ref objectContext);
                if (statusOp == GarnetStatus.OK)
                {
                    if (firstObj.GarnetObject is not SortedSetObject firstSortedSet)
                    {
                        return GarnetStatus.WRONGTYPE;
                    }

                    firstSortedSet.GeoSearch(ref input, ref output, functionsState.respProtocolVersion,
                                             ref opts, true);

                    return GarnetStatus.OK;
                }

                return GarnetStatus.NOTFOUND;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// Geospatial search and store in destination key.
        /// GEOSEARCHSTORE: Store the the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// GEORADIUS (write variant): Store the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center and radius.
        /// GEORADIUSBYMEMBER (write variant): Store the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center (derived from member) and radius.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="destination"></param>
        /// <param name="opts"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus GeoSearchStore<TObjectContext>(ArgSlice key, ArgSlice destination,
                                                                  ref GeoSearchOptions opts,
                                                                  ref ObjectInput input,
                                                                  ref SpanByteAndMemory output,
                                                                  ref TObjectContext objectContext)
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

            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);

            try
            {
                SpanByteAndMemory searchOutMem = default;

                var status = GET(key.ToArray(), out var firstObj, ref objectStoreLockableContext);
                if (status == GarnetStatus.OK)
                {
                    if (firstObj.GarnetObject is SortedSetObject firstSortedSet)
                    {
                        firstSortedSet.GeoSearch(ref input, ref searchOutMem, functionsState.respProtocolVersion,
                                                 ref opts, false);
                    }
                    else
                    {
                        status = GarnetStatus.WRONGTYPE;
                    }
                }

                if (status == GarnetStatus.WRONGTYPE)
                {
                    return GarnetStatus.WRONGTYPE;
                }

                if (status == GarnetStatus.NOTFOUND)
                {
                    // Expire/Delete the destination key if the source key is not found
                    _ = EXPIRE(destination, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None, ref lockableContext, ref objectStoreLockableContext);
                    writer.WriteInt32(0);
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
                        writer.WriteError(error);
                        return GarnetStatus.OK;
                    }

                    var destinationKey = destination.ToArray();
                    objectStoreLockableContext.Delete(ref destinationKey);

                    RespReadUtils.TryReadUnsignedArrayLength(out var foundItems, ref currOutPtr, endOutPtr);

                    // Prepare the parse state for sorted set add
                    parseState.Initialize(foundItems * 2);

                    for (var j = 0; j < foundItems; j++)
                    {
                        RespReadUtils.TryReadUnsignedArrayLength(out var innerLength, ref currOutPtr, endOutPtr);
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

                    var zAddOutput = new GarnetObjectStoreOutput();
                    RMWObjectStoreOperationWithOutput(destinationKey, ref zAddInput, ref objectStoreLockableContext, ref zAddOutput);

                    writer.WriteInt32(foundItems);
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
            }
        }
    }
}