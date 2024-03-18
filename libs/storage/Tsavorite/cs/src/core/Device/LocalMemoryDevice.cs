// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    unsafe struct IORequestLocalMemory
    {
        public void* srcAddress;
        public void* dstAddress;
        public uint bytes;
        public DeviceIOCompletionCallback callback;
        public object context;
        public long startTime;
    }

    /// <summary>
    /// Local storage device
    /// </summary>
    public sealed unsafe class LocalMemoryDevice : StorageDeviceBase
    {
        readonly byte[][] orig_ram_segments;
        readonly byte*[] ram_segment_ptrs;
        private readonly int num_segments;
        private readonly ConcurrentQueue<IORequestLocalMemory>[] ioQueue;
        private readonly Thread[] ioProcessors;
        private readonly int parallelism;
        private readonly long sz_segment;
        private readonly long latencyTicks;
        private bool terminated;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="sz_segment">The size of each segment</param>
        /// <param name="parallelism">Number of IO processing threads</param>
        /// <param name="latencyMs">Induced callback latency in ms (for testing purposes)</param>
        /// <param name="sector_size">Sector size for device (default 64)</param>
        /// <param name="fileName">Virtual path for the device</param>
        public unsafe LocalMemoryDevice(long capacity, long sz_segment, int parallelism, int latencyMs = 0, uint sector_size = 64, string fileName = "/userspace/ram/storage")
            : base(fileName, sector_size, capacity)
        {
            if (capacity == Devices.CAPACITY_UNSPECIFIED) throw new Exception("Local memory device must have a capacity!");
            Debug.WriteLine("LocalMemoryDevice: Creating a " + capacity + " sized local memory device.");
            num_segments = (int)(capacity / sz_segment);
            this.sz_segment = sz_segment;
            latencyTicks = latencyMs * TimeSpan.TicksPerMillisecond;

            ram_segment_ptrs = new byte*[num_segments];
            orig_ram_segments = new byte[num_segments][];


            for (int i = 0; i < num_segments; i++)
            {
                orig_ram_segments[i] = GC.AllocateArray<byte>((int)sz_segment, true);
                ram_segment_ptrs[i] = (byte*)Unsafe.AsPointer(ref orig_ram_segments[i][0]);
            }
            terminated = false;
            ioQueue = new ConcurrentQueue<IORequestLocalMemory>[parallelism];
            this.parallelism = parallelism;
            ioProcessors = new Thread[parallelism];
            for (int i = 0; i != parallelism; i++)
            {
                var x = i;
                ioQueue[x] = new ConcurrentQueue<IORequestLocalMemory>();
                ioProcessors[i] = new Thread(() => ProcessIOQueue(ioQueue[x]));
                ioProcessors[i].Start();
            }

            Debug.WriteLine("LocalMemoryDevice: " + ram_segment_ptrs.Length + " pinned in-memory segments created, each with " + sz_segment + " bytes");
        }

        private void ProcessIOQueue(ConcurrentQueue<IORequestLocalMemory> q)
        {
            while (terminated == false)
            {
                while (q.TryDequeue(out IORequestLocalMemory req))
                {
                    if (latencyTicks > 0)
                    {
                        long timeLeft = latencyTicks - (DateTime.UtcNow.Ticks - req.startTime);
                        if (timeLeft > 0) Thread.Sleep((int)(timeLeft / TimeSpan.TicksPerMillisecond));
                    }
                    Buffer.MemoryCopy(req.srcAddress, req.dstAddress, req.bytes, req.bytes);
                    req.callback(0, req.bytes, req.context);
                }
                Thread.Yield();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
            var q = ioQueue[segmentId % parallelism];
            var req = new IORequestLocalMemory
            {
                srcAddress = ram_segment_ptrs[segmentId] + sourceAddress,
                dstAddress = (void*)destinationAddress,
                bytes = readLength,
                callback = callback,
                context = context
            };
            if (latencyTicks > 0) req.startTime = DateTime.UtcNow.Ticks;
            q.Enqueue(req);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override void WriteAsync(IntPtr sourceAddress,
                                      int segmentId,
                                      ulong destinationAddress,
                                      uint numBytesToWrite,
                                      DeviceIOCompletionCallback callback,
                                      object context)
        {
            Debug.Assert(destinationAddress + numBytesToWrite <= (ulong)sz_segment, "Out of space in segment - LocalMemoryDevice does not support variable-sized segments needed for the object log");

            // We ensure capability of writing to next segment, because there is no
            // extra buffer space allocated in this device
            HandleCapacity(segmentId + 1);

            var q = ioQueue[segmentId % parallelism];
            var req = new IORequestLocalMemory
            {
                srcAddress = (void*)sourceAddress,
                dstAddress = ram_segment_ptrs[segmentId % parallelism] + destinationAddress,
                bytes = numBytesToWrite,
                callback = callback,
                context = context
            };
            if (latencyTicks > 0) req.startTime = DateTime.UtcNow.Ticks;
            q.Enqueue(req);
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            RemoveSegment(segment);
            callback(result);
        }

        /// <summary>
        /// Close device
        /// </summary>
        public override void Dispose()
        {
            foreach (var q in ioQueue)
                while (q.Count != 0) { }
            terminated = true;
            for (int i = 0; i != ioProcessors.Length; i++)
                ioProcessors[i].Join();
        }
    }
}