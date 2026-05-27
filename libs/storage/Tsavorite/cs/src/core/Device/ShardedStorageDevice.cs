// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface that encapsulates a sharding strategy that is used by <see cref="ShardedStorageDevice"/>. This
    /// allows users to customize their sharding behaviors. Some default implementations are supplied for common
    /// partitioning schemes.
    /// </summary>
    interface IPartitionScheme
    {
        /// <summary>
        /// A list of <see cref="IDevice"/> that represents the shards. Indexes into this list will be
        /// used as unique identifiers for the shards.
        /// </summary>
        IList<IDevice> Devices { get; }

        /// <summary>
        /// Maps a range in the unified logical address space into a contiguous physical chunk on a shard's address space.
        /// Because the given range may be sharded across multiple devices, only the largest contiguous chunk starting from
        /// start address but smaller than end address is returned in shard, shardStartAddress, and shardEndAddress.
        /// </summary>
        /// <param name="startAddress">start address of the range to map in the logical address space</param>
        /// <param name="endAddress">end address of the range to map in the logical address space</param>
        /// <param name="shard"> the shard (potentially part of) the given range resides in, given as index into <see cref="Devices"/></param>
        /// <param name="shardStartAddress"> start address translated into physical start address on the returned shard </param>
        /// <param name="shardEndAddress">
        /// physical address of the end of the part of the range on the returned shard. This is not necessarily a translation of the end address
        /// given, as the tail of the range maybe on (a) different device(s).
        /// </param>
        /// <returns>
        /// the logical address translated from the returned shardEndAddress. If this is not equal to the given end address, the caller is
        /// expected to repeatedly call this method using the returned value as the new startAddress until the entire original range is
        /// covered.
        /// </returns>
        long MapRange(long startAddress, long endAddress, out int shard, out long shardStartAddress, out long shardEndAddress);

        /// <summary>
        /// Maps the sector size of a composed device into sector sizes for each shard
        /// </summary>
        /// <param name="sectorSize">sector size of the composed device</param>
        /// <param name="shard">the shard</param>
        /// <returns>sector size on shard</returns>
        long MapSectorSize(long sectorSize, int shard);
    }

    /// <summary>
    /// Uniformly shards data across given devices.
    /// </summary>
    class UniformPartitionScheme : IPartitionScheme
    {
        public IList<IDevice> Devices { get; }
        private readonly long chunkSize;

        /// <summary>
        /// Constructs a UniformPartitionScheme to shard data uniformly across given devices. Suppose we have 3 devices and the following logical write:
        /// [chunk 1][chunk 2][chunk 3][chunk 4]...
        /// chunk 1 is written on device 0, 2 on device 1, 3 on device 2, 4 on device 0, etc.
        /// </summary>
        /// <param name="chunkSize">size of each chunk</param>
        /// <param name="devices">the devices to compose from</param>
        public UniformPartitionScheme(long chunkSize, IList<IDevice> devices)
        {
            Debug.Assert(devices.Count != 0, "There cannot be zero shards");
            Debug.Assert(chunkSize > 0, "chunk size should not be negative");
            Debug.Assert((chunkSize & (chunkSize - 1)) == 0, "Chunk size must be a power of 2");
            Devices = devices;
            this.chunkSize = chunkSize;
            foreach (IDevice device in Devices)
            {
                Debug.Assert(chunkSize % device.SectorSize == 0, "A single device sector cannot be partitioned");
            }
        }

        /// <summary>
        /// vararg version of <see cref="UniformPartitionScheme(long, IList{IDevice})"/>
        /// </summary>
        /// <param name="chunkSize"></param>
        /// <param name="devices"></param>
        public UniformPartitionScheme(long chunkSize, params IDevice[] devices) : this(chunkSize, (IList<IDevice>)devices)
        {
        }

        /// <summary>
        /// <see cref="IPartitionScheme.MapRange(long, long, out int, out long, out long)"/>
        /// </summary>
        /// <param name="startAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="shard"></param>
        /// <param name="shardStartAddress"></param>
        /// <param name="shardEndAddress"></param>
        /// <returns></returns>
        public long MapRange(long startAddress, long endAddress, out int shard, out long shardStartAddress, out long shardEndAddress)
        {
            long chunkId = startAddress / chunkSize;
            shard = (int)(chunkId % Devices.Count);
            shardStartAddress = chunkId / Devices.Count * chunkSize + startAddress % chunkSize;
            long chunkEndAddress = (chunkId + 1) * chunkSize;
            if (endAddress > chunkEndAddress)
            {
                shardEndAddress = shardStartAddress + chunkSize;
                return chunkEndAddress;
            }
            else
            {
                shardEndAddress = endAddress - startAddress + shardStartAddress;
                return endAddress;
            }
        }

        /// <summary>
        /// <see cref="IPartitionScheme.MapSectorSize(long, int)"/>
        /// </summary>
        /// <param name="sectorSize"></param>
        /// <param name="shard"></param>
        /// <returns></returns>
        public long MapSectorSize(long sectorSize, int shard)
        {
            var numChunks = sectorSize / chunkSize;
            // ceiling of (a div b) is (a + b - 1) / b where div is mathematical division and / is integer division 
            return (numChunks + Devices.Count - 1) / Devices.Count * chunkSize;
        }
    }

    /// <summary>
    /// A <see cref="ShardedStorageDevice"/> logically composes multiple <see cref="IDevice"/> into a single storage device
    /// by sharding writes into different devices according to a supplied <see cref="IPartitionScheme"/>. The goal is to be
    /// able to issue large reads and writes in parallel into multiple devices and improve throughput. Beware that this
    /// code does not contain error detection or correction mechanism to cope with increased failure from more devices.
    /// </summary>
    class ShardedStorageDevice : StorageDeviceBase
    {
        private readonly IPartitionScheme partitions;

        /// <summary>
        /// Constructs a new ShardedStorageDevice with the given partition scheme
        /// </summary>
        /// <param name="partitions"> The parition scheme to use </param>
        public ShardedStorageDevice(IPartitionScheme partitions) : base("", 512, -1)
        {
            this.partitions = partitions;
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            foreach (IDevice device in partitions.Devices)
            {
                device.Dispose();
            }
        }

        /// <summary>
        /// <see cref="IDevice.Initialize(long, LightEpoch, bool)"/>
        /// </summary>
        /// <param name="segmentSize"></param>
        /// <param name="epoch"></param>
        /// <param name="omitSegmentIdFromFilename"></param>
        public override void Initialize(long segmentSize, LightEpoch epoch, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);

            for (int i = 0; i < partitions.Devices.Count; i++)
            {
                partitions.Devices[i].Initialize(partitions.MapSectorSize(segmentSize, 0), epoch);
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            var countdown = new CountdownEvent(partitions.Devices.Count);
            foreach (IDevice shard in partitions.Devices)
            {
                shard.RemoveSegmentAsync(segment, ar =>
                {
                    if (countdown.Signal())
                    {
                        callback(ar);
                        countdown.Dispose();
                    }
                }, result);
            }
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, DeviceIOCompletionCallback, object)"/>
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override unsafe void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            // Starts off in one, in order to prevent some issued writes calling the callback before all parallel writes are issued.
            var countdown = new CountdownEvent(1);
            long currentWriteStart = (long)destinationAddress;
            long writeEnd = currentWriteStart + (long)numBytesToWrite;
            uint aggregateErrorCode = 0;
            while (currentWriteStart < writeEnd)
            {
                long newStart = partitions.MapRange(currentWriteStart, writeEnd, out int shard, out long shardStartAddress, out long shardEndAddress);
                ulong writeOffset = (ulong)currentWriteStart - destinationAddress;
                // Indicate that there is one more task to wait for
                countdown.AddCount();
                // Because more than one device can return with an error, it is important that we remember the most recent error code we saw. (It is okay to only
                // report one error out of many. It will be as if we failed on that error and cancelled all other reads, even though we issue reads in parallel and
                // wait until all of them are complete in the implementation) 
                // Can there be races on async result as we issue writes or reads in parallel?
                partitions.Devices[shard].WriteAsync(IntPtr.Add(sourceAddress, (int)writeOffset),
                                                     segmentId,
                                                     (ulong)shardStartAddress,
                                                     (uint)(shardEndAddress - shardStartAddress),
                                                     (e, n, o) =>
                                                     {
                                                         // TODO: Check if it is incorrect to ignore o
                                                         if (e != 0) aggregateErrorCode = e;
                                                         if (countdown.Signal())
                                                         {
                                                             callback(aggregateErrorCode, n, o);
                                                             countdown.Dispose();
                                                         }
                                                     },
                                                     context);

                currentWriteStart = newStart;
            }

            // TODO: Check if overlapped wrapper is handled correctly
            if (countdown.Signal())
            {
                callback(aggregateErrorCode, numBytesToWrite, context);
                countdown.Dispose();
            }
        }

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, DeviceIOCompletionCallback, object)"/>
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            // Starts off in one, in order to prevent some issued writes calling the callback before all parallel writes are issued.
            var countdown = new CountdownEvent(1);
            long currentReadStart = (long)sourceAddress;
            long readEnd = currentReadStart + readLength;
            uint aggregateErrorCode = 0;
            while (currentReadStart < readEnd)
            {
                long newStart = partitions.MapRange(currentReadStart, readEnd, out int shard, out long shardStartAddress, out long shardEndAddress);
                ulong writeOffset = (ulong)currentReadStart - sourceAddress;
                // Because more than one device can return with an error, it is important that we remember the most recent error code we saw. (It is okay to only
                // report one error out of many. It will be as if we failed on that error and cancelled all other reads, even though we issue reads in parallel and
                // wait until all of them are complete in the implementation) 
                countdown.AddCount();
                partitions.Devices[shard].ReadAsync(segmentId,
                                                    (ulong)shardStartAddress,
                                                    IntPtr.Add(destinationAddress, (int)writeOffset),
                                                    (uint)(shardEndAddress - shardStartAddress),
                                                    (e, n, o) =>
                                                    {
                                                        // TODO: this is incorrect if returned "bytes" written is allowed to be less than requested like POSIX.
                                                        if (e != 0) aggregateErrorCode = e;
                                                        if (countdown.Signal())
                                                        {
                                                            callback(aggregateErrorCode, n, o);
                                                            countdown.Dispose();
                                                        }
                                                    },
                                                    context);

                currentReadStart = newStart;
            }

            // TODO: Check handling of overlapped wrapper
            if (countdown.Signal())
            {
                callback(aggregateErrorCode, readLength, context);
                countdown.Dispose();
            }
        }
    }
}