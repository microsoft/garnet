// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.client
{
    enum TaskType : int
    {
        None,
        StringCallback,
        MemoryByteCallback,
        StringArrayCallback,
        MemoryByteArrayCallback,
        StringAsync,
        StringArrayAsync,
        MemoryByteAsync,
        MemoryByteArrayAsync,
        LongAsync,
        LongCallback,
    }

    /// <summary>
    /// Result type for Garnet
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    struct TcsWrapper
    {
        [FieldOffset(0)]
        public TaskCompletionSource<string> stringTcs;
        [FieldOffset(0)]
        public TaskCompletionSource<string[]> stringArrayTcs;
        [FieldOffset(0)]
        public TaskCompletionSource<long> longTcs;
        [FieldOffset(0)]
        public TaskCompletionSource<MemoryResult<byte>> memoryByteTcs;
        [FieldOffset(0)]
        public TaskCompletionSource<MemoryResult<byte>[]> memoryByteArrayTcs;
        [FieldOffset(0)]
        public Action<long, string> stringCallback;
        [FieldOffset(0)]
        public Action<long, MemoryResult<byte>> memoryByteCallback;
        [FieldOffset(0)]
        public Action<long, long, string> longCallback;
        [FieldOffset(0)]
        public Action<long, string[], string> stringArrayCallback;
        [FieldOffset(0)]
        public Action<long, MemoryResult<byte>[], MemoryResult<byte>> memoryByteArrayCallback;
        [FieldOffset(8)]
        public TaskType taskType;
        [FieldOffset(12)]
        public long context;
        [FieldOffset(20)]
        public long timestamp;
        [FieldOffset(28)]
        public int nextTaskId;

        /// <summary>
        /// Load TCS wrapper from given source
        /// </summary>
        /// <param name="source"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LoadFrom(TcsWrapper source)
        {
            switch (source.taskType)
            {
                case TaskType.None:
                    break;
                case TaskType.StringCallback:
                    stringCallback = source.stringCallback;
                    break;
                case TaskType.MemoryByteCallback:
                    memoryByteCallback = source.memoryByteCallback;
                    break;
                case TaskType.StringAsync:
                    stringTcs = source.stringTcs;
                    break;
                case TaskType.StringArrayAsync:
                    stringArrayTcs = source.stringArrayTcs;
                    break;
                case TaskType.MemoryByteAsync:
                    memoryByteTcs = source.memoryByteTcs;
                    break;
                case TaskType.MemoryByteArrayAsync:
                    memoryByteArrayTcs = source.memoryByteArrayTcs;
                    break;
                case TaskType.StringArrayCallback:
                    stringArrayCallback = source.stringArrayCallback;
                    break;
                case TaskType.MemoryByteArrayCallback:
                    memoryByteArrayCallback = source.memoryByteArrayCallback;
                    break;
                case TaskType.LongAsync:
                    longTcs = source.longTcs;
                    break;
                case TaskType.LongCallback:
                    longCallback = source.longCallback;
                    break;
            }
            context = source.context;
            timestamp = source.timestamp;
            taskType = source.taskType;
            //NOTE: prevTaskId should be set last
            nextTaskId = source.nextTaskId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsNext(int taskId) => taskId == nextTaskId;
    }
}