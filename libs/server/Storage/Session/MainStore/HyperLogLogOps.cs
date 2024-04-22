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

        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as key.
        /// </summary>
        public unsafe GarnetStatus HyperLogLogAdd<TContext>(ArgSlice key, string[] elements, out bool updated, ref TContext context)
             where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            updated = false;
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int) + sizeof(long);
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.PFADD;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;

            //cmd args
            *(int*)pcurr = 1; pcurr += sizeof(int);
            byte* output = stackalloc byte[1];
            byte pfaddUpdated = 0;

            for (int i = 0; i < elements.Length; i++)
            {
                var bString = Encoding.ASCII.GetBytes(elements[i]);
                fixed (byte* ptr = bString)
                {
                    *(long*)pcurr = (long)HashUtils.MurmurHash2x64A(ptr, bString.Length);
                    var o = new SpanByteAndMemory(output, 1);
                    var keySB = key.SpanByte;
                    RMW_MainStore(ref keySB, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o, ref context);
                }

                //Invalid HLL Type
                if (*output == (byte)0xFF)
                {
                    pfaddUpdated = 0;
                    break;
                }
                pfaddUpdated |= *output;
            }

            updated = pfaddUpdated > 0;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Adds one element to the HyperLogLog data structure stored at the variable name specified.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public GarnetStatus HyperLogLogAdd<TContext>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, ref TContext context)
          where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
            => RMW_MainStore(ref key, ref input, ref output, ref context);

        /// <summary>
        /// Returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified key,
        /// or 0 if the key does not exist.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="input"></param>
        /// <param name="count"></param>
        /// <param name="error"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HyperLogLogLength<TContext>(ArgSlice[] keys, ref SpanByte input, out long count, out bool error, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            count = 0;
            error = false;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            byte* output = stackalloc byte[sizeof(long)];
            var o = new SpanByteAndMemory(output, sizeof(long));

            for (int i = 0; i < keys.Length; i++)
            {
                SpanByte srcKey = keys[i].SpanByte;
                var status = GET(ref srcKey, ref input, ref o, ref context);
                //Invalid HLL Type
                if (*(long*)(o.SpanByte.ToPointer()) == -1)
                {
                    error = true;
                    return status;
                }

                if (status == GarnetStatus.OK)
                    count += *(long*)(o.SpanByte.ToPointer());
            }

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus HyperLogLogLength<TContext>(ArgSlice[] keys, out long count, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        {
            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            int inputSize = sizeof(int) + RespInputHeader.Size;
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.PFCOUNT;
            (*(RespInputHeader*)(pcurr)).flags = 0;

            return HyperLogLogLength(keys, ref Unsafe.AsRef<SpanByte>(pbCmdInput), out count, out bool error, ref context);
        }

        /// <summary>
        /// Merge multiple HyperLogLog values into a unique value that will approximate the cardinality 
        /// of the union of the observed Sets of the source HyperLogLog structures.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HyperLogLogMerge(ArgSlice[] keys, out bool error)
        {
            error = false;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            bool createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(keys[0], false, LockType.Exclusive);
                for (int i = 1; i < keys.Length; i++)
                    txnManager.SaveKeyEntryToLock(keys[i], false, LockType.Shared);
                txnManager.Run(true);
            }

            var lockableContext = txnManager.LockableContext;

            try
            {
                //4 byte length of input
                //1 byte RespCommand
                //1 byte RespInputFlags
                //4 byte length of HLL read for merging
                int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int);

                sectorAlignedMemoryHll ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment, sectorAlignedMemoryPoolAlignment);
                byte* readBuffer = sectorAlignedMemoryHll.GetValidPointer() + inputSize;
                byte* pbCmdInput = null;
                SpanByte dstKey = keys[0].SpanByte;
                for (int i = 1; i < keys.Length; i++)
                {
                    #region readSrcHLL
                    //build input
                    pbCmdInput = readBuffer - (inputSize - sizeof(int));
                    byte* pcurr = pbCmdInput;
                    *(int*)pcurr = RespInputHeader.Size;
                    pcurr += sizeof(int);
                    //1. header
                    (*(RespInputHeader*)(pcurr)).cmd = RespCommand.PFMERGE;
                    (*(RespInputHeader*)(pcurr)).flags = 0;

                    SpanByteAndMemory mergeBuffer = new SpanByteAndMemory(readBuffer, hllBufferSize);
                    var srcKey = keys[i].SpanByte;
                    var status = GET(ref srcKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref mergeBuffer, ref lockableContext);
                    //Handle case merging source key does not exist
                    if (status == GarnetStatus.NOTFOUND)
                        continue;
                    //Invalid Type
                    if (*(long*)readBuffer == -1)
                    {
                        error = true;
                        break;
                    }
                    #endregion
                    #region mergeToDst
                    pbCmdInput = readBuffer - inputSize;
                    pcurr = pbCmdInput;
                    *(int*)pcurr = inputSize - sizeof(int) + mergeBuffer.Length;
                    pcurr += sizeof(int);
                    (*(RespInputHeader*)(pcurr)).cmd = RespCommand.PFMERGE;
                    (*(RespInputHeader*)(pcurr)).flags = 0;
                    pcurr += RespInputHeader.Size;
                    *(int*)pcurr = mergeBuffer.Length;
                    SET_Conditional(ref dstKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref mergeBuffer, ref lockableContext);
                    #endregion
                }
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
            return GarnetStatus.OK;
        }
    }
}