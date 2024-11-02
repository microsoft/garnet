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
        public unsafe GarnetStatus HyperLogLogAdd<TKeyLocker, TEpochGuard>(ArgSlice key, string[] elements, out bool updated)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            updated = false;
            var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int) + sizeof(long);
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //header
            (*(RespInputHeader*)pcurr).cmd = RespCommand.PFADD;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;

            //cmd args
            *(int*)pcurr = 1; pcurr += sizeof(int);
            var output = stackalloc byte[1];
            byte pfaddUpdated = 0;

            for (var i = 0; i < elements.Length; i++)
            {
                var bString = Encoding.ASCII.GetBytes(elements[i]);
                fixed (byte* ptr = bString)
                {
                    *(long*)pcurr = (long)HashUtils.MurmurHash2x64A(ptr, bString.Length);
                    var sbam = new SpanByteAndMemory(output, 1);
                    var keySB = key.SpanByte;
                    _ = RMW_MainStore<TKeyLocker, TEpochGuard>(ref keySB, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref sbam);
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
        public GarnetStatus HyperLogLogAdd<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMW_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <summary>
        /// Returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified key,
        /// or 0 if the key does not exist.
        /// </summary>
        public unsafe GarnetStatus HyperLogLogLength<TKeyLocker, TEpochGuard>(Span<ArgSlice> keys, ref SpanByte input, out long count, out bool error)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            count = 0;
            error = false;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var output = stackalloc byte[sizeof(long)];
            var sbam = new SpanByteAndMemory(output, sizeof(long));

            for (var i = 0; i < keys.Length; i++)
            {
                var srcKey = keys[i].SpanByte;
                var status = GET<TKeyLocker, TEpochGuard>(ref srcKey, ref input, ref sbam);
                //Invalid HLL Type
                if (*(long*)sbam.SpanByte.ToPointer() == -1)
                {
                    error = true;
                    return status;
                }

                if (status == GarnetStatus.OK)
                    count += *(long*)sbam.SpanByte.ToPointer();
            }

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus HyperLogLogLength<TKeyLocker, TEpochGuard>(Span<ArgSlice> keys, out long count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            var inputSize = sizeof(int) + RespInputHeader.Size;
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.PFCOUNT;
            (*(RespInputHeader*)pcurr).flags = 0;

            return HyperLogLogLength<TKeyLocker, TEpochGuard>(keys, ref Unsafe.AsRef<SpanByte>(pbCmdInput), out count, out var error);
        }

        /// <summary>
        /// Merge multiple HyperLogLog values into a unique value that will approximate the cardinality 
        /// of the union of the observed Sets of the source HyperLogLog structures.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HyperLogLogMerge(Span<ArgSlice> keys, out bool error)
        {
            error = false;
            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                _ = txnManager.SaveKeyEntryToLock(keys[0], false, LockType.Exclusive);
                for (var i = 1; i < keys.Length; i++)
                    _ = txnManager.SaveKeyEntryToLock(keys[i], false, LockType.Shared);
                createTransaction = txnManager.Run(internal_txn: true);
            }

            try
            {
                //4 byte length of input
                //1 byte RespCommand
                //1 byte RespInputFlags
                //4 byte length of HLL read for merging
                var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int);

                sectorAlignedMemoryHll ??= new SectorAlignedMemory(hllBufferSize + sectorAlignedMemoryPoolAlignment, sectorAlignedMemoryPoolAlignment);
                var readBuffer = sectorAlignedMemoryHll.GetValidPointer() + inputSize;
                byte* pbCmdInput = null;
                var dstKey = keys[0].SpanByte;
                for (var i = 1; i < keys.Length; i++)
                {
                    #region readSrcHLL
                    //build input
                    pbCmdInput = readBuffer - (inputSize - sizeof(int));
                    var pcurr = pbCmdInput;
                    *(int*)pcurr = RespInputHeader.Size;
                    pcurr += sizeof(int);
                    //1. header
                    (*(RespInputHeader*)pcurr).cmd = RespCommand.PFMERGE;
                    (*(RespInputHeader*)pcurr).flags = 0;

                    SpanByteAndMemory mergeBuffer = new (readBuffer, hllBufferSize);
                    var srcKey = keys[i].SpanByte;
                    var status = GET<TransactionalSessionLocker, GarnetSafeEpochGuard>(ref srcKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref mergeBuffer);
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
                    (*(RespInputHeader*)pcurr).cmd = RespCommand.PFMERGE;
                    (*(RespInputHeader*)pcurr).flags = 0;
                    pcurr += RespInputHeader.Size;
                    *(int*)pcurr = mergeBuffer.Length;
                    SET_Conditional<TransactionalSessionLocker, GarnetSafeEpochGuard>(ref dstKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref mergeBuffer);
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